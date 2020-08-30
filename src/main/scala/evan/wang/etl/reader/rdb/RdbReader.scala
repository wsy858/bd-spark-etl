package evan.wang.etl.reader.rdb

import java.util.Properties

import evan.wang.etl.option.Options
import evan.wang.etl.reader.BaseReader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s._
import org.json4s.jackson.JsonMethods._


class RdbReader extends BaseReader{

  override def readData(sparkSession: SparkSession, options: Options): DataFrame = {
    val jsonStr: JValue = parse(options.getReaderConfig)//将Json字符串转为 JValue
    implicit val formats: DefaultFormats.type = DefaultFormats
    val rdbConfig: RdbConfig = jsonStr.extract[RdbConfig]
    sparkSession.read.jdbc(rdbConfig.jdbcUrl, rdbConfig.getTableName, new Properties())
  }

}
