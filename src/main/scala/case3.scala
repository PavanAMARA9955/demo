import org.apache.spark.sql.SparkSession

object case3 {
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val file1 = "C:/Users/216527/Documents/dataset/claims.csv"
    val df1 = spark.read.options(Map("inferSchema" -> "true", "sep" -> ",", "header" -> "true")).csv(file1)
    val file2 = "C:/Users/216527/Documents/dataset/claims_revision.csv"
    val df2 = spark.read.options(Map("inferSchema" -> "true", "sep" -> ",", "header" -> "true")).csv(file2)

    df1.createOrReplaceTempView("Claims")
    df2.createOrReplaceTempView("Claims_Revision")

//    df1.show()
//    df2.show()


    val DataFrame = spark.sql("SELECT Claim, Line, Date, Line_amt, FND_CD, " +
      "ROW_NUMBER() OVER (PARTITION BY Claim,Line ORDER BY Date DESC) AS Version, " +
      "CASE WHEN ROW_NUMBER() OVER (PARTITION BY Claim, Line ORDER BY Date DESC) = 1 " +
      "THEN 'Y' " +
      "ELSE 'N' " +
      "END AS Current_Version_Flag FROM Claims")
    DataFrame.createOrReplaceTempView("Output")
    DataFrame.show()

//    spark.sql(
//      """ select Claim, Line, Date, Line_amt, FND_CD,
//        coalesce((select max(version) from Output c2 where c2.Claim = Output.Claim), 0) + 1 as version,
//        case when not exists (select 1 from Output c3 where c3.Claim = Output.Claim and c3.Current_Version_flag = 'Y')
//        then 'Y'
//        else 'N'
//        end as Current_Version_flag
//        from Output""").show()

//    spark.sql(
//      """SELECT Claim, Line, Date, Line_amt, FND_CD,
//     COALESCE((SELECT MAX(version) FROM Output c2 WHERE c2.Claim = o.Claim), 0) + 1 AS version,
//     CASE WHEN NOT EXISTS (SELECT 1 FROM Output c3 WHERE c3.Claim = o.Claim AND c3.Current_Version_flag = 'Y')
//          THEN 'Y'
//          ELSE 'N'
//     END AS Current_Version_flag
//     FROM Output o""").show()

  }

}
