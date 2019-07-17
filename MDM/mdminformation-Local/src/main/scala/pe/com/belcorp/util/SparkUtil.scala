package pe.com.belcorp.util

import org.apache.spark.sql.SparkSession


object SparkUtil {

  def getSparkSession(appName: String): SparkSession = {
    val spark = SparkSession.builder().appName(appName).master("local[4]").getOrCreate()
// PARA SERVIDOR
    //val spark = SparkSession.builder().appName(appName).getOrCreate()

    //CONFIG TO REDSHIFT - S3 (LOCAL MODE)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "AKIA356NXYFCB4CQTP5N")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "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")
    spark
  }
}
