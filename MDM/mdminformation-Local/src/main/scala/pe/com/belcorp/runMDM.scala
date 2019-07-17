package pe.com.belcorp

import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import pe.com.belcorp.tables.SourceProcess
import pe.com.belcorp.util.Arguments
import pe.com.belcorp.util.SparkUtil._
import pe.com.belcorp.util.AWSCredentials

object runMDM {

  System.setProperty("hadoop.home.dir", "C:\\hadoop")
  //2 primera lineas para MONGO QAS
  val readConfig = ReadConfig(Map("uri" -> "mongodb+srv://adminBDInfoServiceQAS:FtzDmZ1hLx2yAFsU@bigdatainfoservice-0hd3l.mongodb.net/info_product.fichaproducto2?readPreference=primaryPreferred"))
  val writeConfig = WriteConfig(Map("uri" -> "mongodb+srv://adminBDInfoServiceQAS:FtzDmZ1hLx2yAFsU@bigdatainfoservice-0hd3l.mongodb.net/info_product.fichaproducto2"))
  //val readConfig = ReadConfig(Map("uri" -> "mongodb+srv://userBDInfoServicePRD:7A0BMQ3YHS1gTSg0@bigdatainfoservice-8x89f.mongodb.net/info_product.fichaproducto?readPreference=primaryPreferred"))
  //val writeConfig = WriteConfig(Map("uri" -> "mongodb+srv://userBDInfoServicePRD:7A0BMQ3YHS1gTSg0@bigdatainfoservice-8x89f.mongodb.net/info_product.fichaproducto"))

  def main(args: Array[String]): Unit = {
    val params = new Arguments(args)
    val spark = getSparkSession("MDM-info")
    val process = new SourceProcess(spark, params)
    params.source() match {
      case "comunicaciones" => process.executeSource("comunicaciones")
      case "sap" => process.executeSource("sap")
      case "webRedes" => process.executeSource("webRedes")
      case "all" => process.executeSource("comunicaciones")
        process.executeSource("webRedes")
        process.executeSource("sap")
      case _ => println("NOTHING TO DO")
    }
    spark.stop()
  }
 }
