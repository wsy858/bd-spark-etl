package evan.wang.etl.writer.hive

import evan.wang.etl.writer.BaseWriter
import evan.wang.etl.option.Options
import org.apache.spark.sql.{DataFrame, SparkSession}


class HiveWriter extends BaseWriter {

  override def writeData(sparkSession: SparkSession,options: Options, data: DataFrame): Unit = {
  }

}
