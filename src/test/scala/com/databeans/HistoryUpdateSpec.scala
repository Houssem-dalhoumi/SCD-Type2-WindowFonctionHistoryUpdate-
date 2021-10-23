package com.databeans

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lead, lit}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import com.databeans.HistoryUpdate._
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.time.LocalDate
import java.time.format.DateTimeFormatter


case class AddressHistory
(id: Long, first_name: String, last_name: String, address: String, moved_in: LocalDate, moved_out: LocalDate, current: Boolean)

case class AddressUpdates(id: Long, first_name: String, last_name: String, address: String, moved_in: LocalDate)


class HistoryUpdateSpec extends AnyFlatSpec with GivenWhenThen with Matchers {

  implicit val spark : SparkSession = SparkSession
    .builder()
    .appName("Update_test")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val pattern = DateTimeFormatter.ofPattern("dd-MM-yyyy")


  val addressHistory = Seq(
    AddressHistory(1, "Boazizi", "Sayf", "Soussa", LocalDate.parse("01-06-2020", pattern), null, true),
    AddressHistory(2, "abidi", "jacer", "amesterdam", LocalDate.parse("01-02-2020", pattern), null, true),
    AddressHistory(3, "banneni", "oussema", "paris", LocalDate.parse("05-06-2019", pattern), null, true),
    AddressHistory(2, "abidi", "jacer", "kasserine", LocalDate.parse("01-02-1995", pattern), LocalDate.parse("01-02-2020", pattern), false),
    AddressHistory(4, "dalhoumi", "houssem", "kasserine", LocalDate.parse("01-12-1992", pattern), LocalDate.parse("01-02-2020", pattern), false),
      AddressHistory(4, "dalhoumi", "houssem", "tunis", LocalDate.parse("01-12-2020", pattern), null, true)

  ).toDF()
  val addressUpdate = Seq(
    AddressUpdates(1, "Boazizi", "Sayf", "Kasserine", LocalDate.parse("01-09-2020", pattern)),
    AddressUpdates(2, "abidi", "jacer", "amesterdam", LocalDate.parse("01-01-2020", pattern)),
    AddressUpdates(3, "banneni", "oussema", "kasserine", LocalDate.parse("01-08-1992", pattern)),
    AddressUpdates(4, "dalhoumi", "houssem", "boudiryess", LocalDate.parse("01-08-2019", pattern)),
    AddressUpdates(5, "abidi", "ghassen", "tatawin", LocalDate.parse("01-08-2020", pattern))
  ).toDF()



  "addressHistoryBuilder" should "update the history" in {
    Given("the history and the update ")
    val history = addressHistory
    val update = addressUpdate
    When("addressHistoryBuilder is invoked")
    val historyBuild = addressHistoryBuilder(history,update)
    Then("the history should be updated ")
    val expectedResult = Seq(
      AddressHistory(1, "Boazizi", "Sayf", "Soussa", LocalDate.parse("01-06-2020", pattern), LocalDate.parse("01-09-2020", pattern), false),
      AddressHistory(1, "Boazizi", "Sayf", "Kasserine", LocalDate.parse("01-09-2020", pattern), null, true),
      AddressHistory(2, "abidi", "jacer", "amesterdam", LocalDate.parse("01-01-2020", pattern), null, true),
      AddressHistory(3, "banneni", "oussema", "paris", LocalDate.parse("05-06-2019", pattern), null, true),
      AddressHistory(3, "banneni", "oussema", "kasserine", LocalDate.parse("01-08-1992", pattern),LocalDate.parse("05-06-2019", pattern) , false),
      AddressHistory(2, "abidi", "jacer", "kasserine", LocalDate.parse("01-02-1995", pattern), LocalDate.parse("01-01-2020", pattern), false),
      AddressHistory(4, "dalhoumi", "houssem", "kasserine", LocalDate.parse("01-12-1992", pattern), LocalDate.parse("01-08-2019", pattern), false),
      AddressHistory(4, "dalhoumi", "houssem", "boudiryess", LocalDate.parse("01-08-2019", pattern), LocalDate.parse("01-12-2020", pattern), false),
      AddressHistory(4, "dalhoumi", "houssem", "tunis", LocalDate.parse("01-12-2020", pattern), null, true),
      AddressHistory(5, "abidi", "ghassen", "tatawin", LocalDate.parse("01-08-2020", pattern), null , true)
    ).toDF()

    historyBuild.collect() should contain theSameElementsAs (expectedResult.collect())



  }

}
