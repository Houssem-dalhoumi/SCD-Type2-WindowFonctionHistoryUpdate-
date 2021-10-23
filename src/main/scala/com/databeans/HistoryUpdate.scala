package com.databeans

import com.databeans.HistoryUpdateUtils._
import org.apache.spark.sql.DataFrame

object HistoryUpdate {

  def addressHistoryBuilder (addressHistory: DataFrame, addressUpdate: DataFrame): DataFrame = {

    val historyAndUpdate = unifyingHistoryAndUpdate(addressHistory,addressUpdate)
    val shouldBeUpdated = historyShouldBeUpdated(historyAndUpdate)
    val updatedMovedOut = updateHistoryMovedOut(shouldBeUpdated)
    val lateDateSameAddress = getLateDateSameAddressRow(updatedMovedOut)
    val lateDateSameAddressCorrected = getLateDateSameAddressRowCorrected(lateDateSameAddress)
    val rowsToBeRemoved = getToBeRemovedRows(lateDateSameAddress)

    val updatedHistory = updatedMovedOut.union(lateDateSameAddressCorrected).except(rowsToBeRemoved)

    getCorrectedCurrent(updatedHistory)

  }

}
