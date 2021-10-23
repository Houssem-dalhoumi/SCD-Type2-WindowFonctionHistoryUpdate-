package com.databeans

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lead, lit}

object HistoryUpdateUtils {


def unifyingHistoryAndUpdate (addressHistory : DataFrame , addressUpdate: DataFrame) : DataFrame = {
  val updateColumnAdd = addressUpdate
    .withColumn("moved_out",lit(null))

  val history = addressHistory
    .drop("current")
  val unifiedHistory = history.union(updateColumnAdd)

  val windowSpec  = Window.partitionBy("id").orderBy("id")
  unifiedHistory.sort("moved_in","moved_out")
    .withColumn("lead",lead("moved_in",1).over(windowSpec))
}

  def historyShouldBeUpdated (rowsWthWrongMovedOut : DataFrame) : DataFrame = {
    rowsWthWrongMovedOut.where((col("moved_out") isNull) || (col("lead") < col("moved_out")))
  }

def updateHistoryMovedOut (historyShouldBeUpdated: DataFrame): DataFrame = {
  historyShouldBeUpdated.withColumn("moved_out", col("lead"))
    .drop("lead")
}

  def getLateDateSameAddressRow (historyMovedOutUpdated : DataFrame): DataFrame = {

    val windowSpec = Window.partitionBy("id").orderBy("id")

     historyMovedOutUpdated.sort("moved_in", "moved_out")
      .withColumn("lead", lead("moved_in", 1) over windowSpec)
      .withColumn("new_moved_out", lead("moved_out", 1) over windowSpec)
      .withColumn("new_address", lead("address", 1) over windowSpec)
      .where((col("lead") === col("moved_out")) && (col("address") === col("new_address")))

  }
    def getLateDateSameAddressRowCorrected (lateDateSameAddressRow: DataFrame): DataFrame = {
    lateDateSameAddressRow.withColumn("moved_out", col("new_moved_out"))
    .drop("new_moved_out", "new_address", "lead")
  }

  def getToBeRemovedRows (lateDateSameAddressRow: DataFrame): DataFrame = {
    val reamoved1 = lateDateSameAddressRow.drop("new_moved_out", "new_address", "lead")

    val removed2 = lateDateSameAddressRow.withColumn("moved_in", col("moved_out"))
      .withColumn("moved_out", lit(null))
      .drop("new_moved_out", "new_address", "lead")

     reamoved1.union(removed2)
  }

  def getCorrectedCurrent (updatedHistory : DataFrame): DataFrame = {
    val addCurrentTrue = updatedHistory
      .where(col("moved_out").isNull)
      .withColumn("current", lit(true))
    val addCrrentFalse = updatedHistory
      .where(col("moved_out").isNotNull)
      .withColumn("current", lit(false))
    addCrrentFalse.union(addCurrentTrue)

  }


}
