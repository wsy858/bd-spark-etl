package evan.wang.etl.writer

import evan.wang.etl.option.Options
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.beans.BeanProperty

trait BaseWriter {
   @BeanProperty var name: String = _
   def writeData(sparkSession: SparkSession, options: Options, data: DataFrame)

}
