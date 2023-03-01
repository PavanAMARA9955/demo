import case2.spark
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StringType
import test.spark

import scala.reflect.runtime.universe.TypeTag

object test2 {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    // Define the UDF
    def getMonthName(n: Int): String = n match {
      case 1 => "January"
      case 2 => "February"
      case 3 => "March"
      case 4 => "April"
      case 5 => "May"
      case 6 => "June"
      case 7 => "July"
      case 8 => "August"
      case 9 => "September"
      case 10 => "October"
      case 11 => "November"
      case 12 => "December"
      case _ => "Invalid month number"
    }

    // Register the UDF
    val getMonthNameUDF: UserDefinedFunction = udf(getMonthName _)
    spark.udf.register("get_month_name_udf", getMonthNameUDF)


  def month_name(): Unit = {
    spark.sql("SELECT STATION_NAME,SUM(NUMBER_OF_CARS_PARKED) AS total_cars,get_month_name_udf(MONTH) AS month_name " +
      "FROM Jointables GROUP BY STATION_NAME, MONTH ORDER BY MONTH").show()
  }

  def parking_across(): Unit = {
    spark.sql("SELECT STATION_NAME, SUM(NUMBER_OF_CARS_PARKED) AS total_cars,YEAR,MONTH " +
      "FROM Jointables GROUP BY STATION_NAME,YEAR,MONTH ORDER BY total_cars DESC").show()

  }

  def total_parked_per_mon(): Unit = {
    spark.sql("WITH cumulative_total AS (SELECT STATION_NAME,SUM(NUMBER_OF_CARS_PARKED) " +
      "AS total_cars,YEAR,MONTH,SUM(SUM(NUMBER_OF_CARS_PARKED)) OVER " +
      "(PARTITION BY STATION_NAME ORDER BY YEAR,MONTH) AS cumulative_sum FROM Jointables " +
      "GROUP BY STATION_NAME,YEAR,MONTH) SELECT STATION_NAME,total_cars,YEAR,MONTH,cumulative_sum " +
      "FROM cumulative_total").show(500)

  }

  def cumu_tot_eachRecord(): Unit = {
    spark.sql("WITH cumulative_total AS (SELECT STATION_NAME,NUMBER_OF_CARS_PARKED,YEAR,MONTH,SUM(NUMBER_OF_CARS_PARKED) " +
      "OVER (ORDER BY YEAR,MONTH) AS cumulative_sum FROM Jointables) " +
      "SELECT STATION_NAME,NUMBER_OF_CARS_PARKED,YEAR,MONTH,cumulative_sum " +
      "FROM cumulative_total").show(500)
  }

  def drug2(): Unit ={
    spark.sql("""
        SELECT patient_uuid, drug, prescriptiondate,
          CASE WHEN prescriptiondate = MIN(prescriptiondate) OVER (PARTITION BY patient_uuid, drug)
          THEN 1 ELSE 0 END AS flag
          FROM (
            SELECT patient_uuid, drug, prescriptiondate
            FROM patientData
            GROUP BY patient_uuid, drug, prescriptiondate
            order by patient_uuid,prescriptiondate)
            """).show(200)
  }

  def drug3(): Unit ={
    spark.sql("""
      SELECT patient_uuid, drug, prescriptiondate,
             CASE WHEN ROW_NUMBER() OVER (PARTITION BY patient_uuid, drug ORDER BY prescriptiondate) = 1
             THEN 1 ELSE 0 END AS flag
      FROM (
        SELECT patient_uuid, drug, prescriptiondate
        FROM patientData
        GROUP BY patient_uuid, drug, prescriptiondate
      )
     """).show(200)
  }

  def drug4()={
    val result= spark.sql(
      """SELECT patient_uuid, drug, prescriptiondate,
        datediff(lag(prescriptiondate, -1) over(PARTITION BY patient_uuid, drug ORDER BY prescriptiondate)
        ,prescriptiondate) AS days
                FROM NewpatientData
              GROUP BY patient_uuid, drug, prescriptiondate
              ORDER BY patient_uuid,prescriptiondate""")

    result.createOrReplaceTempView("NewpatientData1")

    spark.sql("""SELECT patient_uuid, drug, prescriptiondate,coalesce(days,0) AS DaysDiff FROM NewpatientData1""").show(100)
  }

}


