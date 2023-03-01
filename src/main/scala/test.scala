import org.apache.spark.sql.SparkSession

object test{
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate()

  def main(args:Array[String]):Unit= {

    val filePath1="C:/Users/216527/Documents/dataset/Parking_Info.csv"
    val filePath2="C:/Users/216527/Documents/dataset/Station.csv"

    val df1 = spark.read.options(Map("inferSchema"->"true","sep"->",","header"->"true")).csv(filePath1)
    val df2 = spark.read.options(Map("inferSchema"->"true","sep"->",","header"->"true")).csv(filePath2)

//    df1.show(false)
//    df1.printSchema()
//    df2.show(false)
//    df2.printSchema()

    df1.createOrReplaceTempView("Parking_Info")
    df2.createOrReplaceTempView("Station")

//    	Join “station” and “parking_info” dataset and calculate
    val Total = spark.sql(" SELECT station.STATION_iD, station.STATION_NAME, station.NUMBER_OF_SPACES,Parking_Info.Month, Parking_Info.YEAR,COALESCE(Parking_Info.NUMBER_OF_CARS_PARKED, 0) AS NUMBER_OF_CARS_PARKED,COALESCE(Parking_Info.PERCENT_FILLED, 0) AS PERCENT_FILLED  FROM station  INNER JOIN Parking_Info  ON Parking_Info.STATION_ID = station.STATION_ID")
    Total.show()

    Total.createOrReplaceTempView("Jointables")

    test2.month_name()
    test2.parking_across()
    test2.total_parked_per_mon()
    test2.cumu_tot_eachRecord()

  }

}