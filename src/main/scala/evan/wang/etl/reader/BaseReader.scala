package evan.wang.etl.reader

import evan.wang.etl.option.Options
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.beans.BeanProperty

trait BaseReader {
  @BeanProperty var name: String = _
  def readData(sparkSession: SparkSession, options: Options): DataFrame
}
