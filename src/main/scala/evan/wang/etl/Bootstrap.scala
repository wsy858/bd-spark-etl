package evan.wang.etl

import com.alibaba.fastjson.JSON
import evan.wang.etl.reader.ReaderFactory
import evan.wang.etl.reader.mysql.MySqlConfig
import evan.wang.etl.writer.WriterFactory
import evan.wang.etl.option.OptionParser
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import evan.wang.etl.reader.rdb.RdbConfig

/**
  * @author evan wang
  * @todo 计算任务执行入口
  * @version 1.0
  */
object Bootstrap {
  //日志收集器
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)


  /**
    * @todo 创建spark session
    * @return
    */
  private def newSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .appName("bd-spark-etl")
      .master(if ("true".equals(System.getProperty("debug"))) "local[2]" else "yarn")
      .enableHiveSupport()
      .config("spark.scheduler.mode", "FAIR") // 共享模式，平均分配资源
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.task.maxFailures", 10) // 任务失败重试次数
      .config("spark.port.maxRetries", 300) // spark ui服务端口占用重试次数
      .config("spark.ui.port", 4044) // spark ui服务端口
      .getOrCreate()
  }


  /**
    * @todo 任务运行
    * @param args 输入参数
    */
  private def runJob(args: Array[String]): Int = {
    logger.info("main args:{}", args.mkString(","))
    val options = new OptionParser(args).getOptions
    logger.info(options.toString)
    val sparkSession = newSparkSession()
    val reader = ReaderFactory.getReader(options.getReaderName)
    val data = reader.readData(sparkSession, options)
    val writer = WriterFactory.getWrite(options.getWriterName)
    writer.writeData(sparkSession, options, data)
    logger.info("------------任务结束-------------")
    0
  }


  /**
    * @todo 执行入口
    * @param args 执行参数
    */
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      logger.error("Args must not be empty.")
      System.exit(1)
    }
    runJob(args)

  }


}