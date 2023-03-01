import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{coalesce, col, date_add, date_trunc, datediff, lag, to_date}

object case2 {
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val file="C:/Users/216527/Documents/dataset/dataset_don_not_share.csv"
    val df = spark.read.options(Map("inferSchema"->"true","sep"->",","header"->"true")).csv(file)

    df.createOrReplaceTempView("patientData")
//    df.show()

    val dfWithDate = df.withColumn("prescriptiondate", to_date(col("prescriptiondate"),"dd/MM/yyyy"))
//    dfWithDate.show()
    dfWithDate.createOrReplaceTempView("NewpatientData")

//spark.sql("Select * from NewpatientData").show()



//    spark.sql(
//      """
//    SELECT patient_uuid, drug, prescriptiondate, mg, quantity,
//        SUM(total_usage) OVER (PARTITION BY patient_uuid,drug ORDER BY prescriptiondate,drug) AS cumulative_total_usage
//    FROM (
//        SELECT patient_uuid, drug, prescriptiondate, mg, quantity,
//            CASE
//                WHEN DATEDIFF(prescriptiondate, MIN(prescriptiondate) OVER (PARTITION BY patient_uuid)) < 30 THEN mg * quantity
//                ELSE 0
//            END AS total_usage
//        FROM NewpatientData
//        GROUP BY patient_uuid, prescriptiondate, drug, mg, quantity
//        ORDER BY patient_uuid, prescriptiondate
//    )
//    """
//    ).show(100)


        spark.sql(
          """SELECT patient_uuid, drug, prescriptiondate,mg,quantity,
         SUM(CASE
               WHEN prescriptiondate >= DATE_ADD(prescriptiondate, 30) THEN 0
               ELSE (mg * quantity)
             END) OVER (PARTITION BY patient_uuid ORDER BY prescriptiondate) AS cumulative_total_usage
         FROM NewpatientData
         GROUP BY patient_uuid, prescriptiondate, drug, mg,quantity
         ORDER BY patient_uuid, prescriptiondate"""
        ).show(100)




    //    test2.drug2()
//    test2.drug3()
//    test2.drug4()
  }

}










//spark.sql("""SELECT patient_uuid, drug, dosage, mg, date_trunc('day', prescriptiondate) AS month_start,
//             SUM(
//             case (
//             datediff(
//             date_trunc('day', prescriptiondate),prescriptiondate) < 30)
//             THEN return (dosage*mg)
//             ) AS total_dosage FROM NewpatientData
//             GROUP BY patient_uuid, drug, month_start, mg, dosage
//             order by patient_uuid, month_start""").show()

//spark.sql("select patient_uuid,prescriptiondate,sum(mg*dosage) as total_dose ")


//spark.sql("""SELECT patient_uuid, drug, dosage, mg, prescriptiondate,
//             (SELECT SUM(dosage * mg) from NewpatientData m1
//              WHERE EXISTS ( SELECT prescriptiondate FROM NewpatientData m2
//                             WHERE m2.prescriptiondate > m1.prescriptiondate
//                             AND m2.prescriptiondate <= DATEADD(day, 30, m1.prescriptiondate)
//                             )
//              ) AS total_dosage FROM NewpatientData
//     ORDER BY patient_uuid,prescriptiondate
//     """).show()


//    spark.sql("""SELECT patient_uuid,prescriptiondate, SUM(mg * dosage), drug, dosage, mg
//                FROM NewpatientData m1
//                WHERE EXISTS (
//                  SELECT *
//                  FROM NewpatientData m2
//                  WHERE m2.patient_uuid = m1.patient_uuid
//                  AND m2.prescriptiondate > m1.prescriptiondate
//                  AND m2.prescriptiondate <= DATEADD(day, 30, m1.prescriptiondate)
//                  Order BY patient_uuid,prescriptiondate
//                )
//                GROUP BY patient_uuid,prescriptiondate, drug, dosage, mg
//                """).show()





//    spark.sql("SELECT SUM(mg*dosage) FROM NewpatientData m1 WHERE EXISTS (SELECT prescriptiondate FROM NewpatientData m2 WHERE m2.prescriptiondate > m1.prescriptiondate AND m2.prescriptiondate <= DATEADD(day, 30, m1.prescriptiondate))").show()


//    spark.sql(
//      """WITH cumulative_val AS (SELECT patient_uuid, drug, prescriptiondate,mg,Dosage,
//        sum(sum(Dosage*mg)) OVER (PARTITION BY patient_uuid ORDER BY prescriptiondate) AS cumulative_total
//        WHERE datediff(date_trunc('day', prescriptiondate),prescriptiondate) < 30
//        FROM NewpatientData m1
//         WHERE EXISTS ( SELECT prescriptiondate FROM NewpatientData m2
//                        WHERE m2.prescriptiondate > m1.prescriptiondate
//                        AND m2.prescriptiondate <= DATEADD(day, 30, m1.prescriptiondate)
//                        )
//          GROUP BY patient_uuid,prescriptiondate,drug,mg,Dosage
//          )
//        select patient_uuid,drug,mg,Dosage, prescriptiondate,cumulative_total FROM cumulative_val""").show(100)
//


//    spark.sql("""WITH cumulative_val AS (SELECT patient_uuid, drug, to_date(prescriptiondate, 'dd/MM/yyyy')," +
//                |  "SUM(CASE WHEN to_date(prescriptiondate, 'dd/MM/yyyy') >= DATE_ADD(CAST(to_date(prescriptiondate, 'dd/MM/yyyy') AS DATE), 30) THEN 0 " +
//                |  "ELSE (mg * quantity) END) as total_usage, " +
//                |  "sum(sum(Dosage*mg)) OVER (PARTITION BY patient_uuid ORDER BY prescriptiondate) AS cumulative_total " +
//                |  "FROM NewpatientData " +
//                |  "GROUP BY patient_uuid, to_date(prescriptiondate, 'dd/MM/yyyy'), drug " +
//                |  "ORDER BY patient_uuid, to_date(prescriptiondate, 'dd/MM/yyyy')) " +
//                |  "SELECT * FROM cumulative_val """.stripMargin).show(100)


//    spark.sql(
//      """SELECT patient_uuid, drug, to_date(prescriptiondate, 'dd/MM/yyyy') AS Prescription_Date,
//        SUM(CASE WHEN to_date(prescriptiondate, 'dd/MM/yyyy') >= DATE_ADD(CAST(to_date(prescriptiondate, 'dd/MM/yyyy') AS DATE), 30)
//        THEN 0
//        ELSE (mg * quantity)
//        END) as total_usage
//        FROM NewpatientData GROUP BY patient_uuid, to_date(prescriptiondate, 'dd/MM/yyyy'), drug
//        ORDER BY patient_uuid, to_date(prescriptiondate, 'dd/MM/yyyy')""").show(100)


