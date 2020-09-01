package evan.wang.etl

import java.util.concurrent.TimeUnit

import evan.wang.etl.option.OptionParser
import evan.wang.etl.reader.ReaderFactory
import evan.wang.etl.writer.WriterFactory
import org.apache.commons.lang3.time.StopWatch
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

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
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.task.maxFailures", 10) // 任务失败重试次数
      .config("spark.port.maxRetries", 100) // spark ui服务端口占用重试次数
      .config("spark.network.timeout", 3600) // 网络超时时间
      .config("spark.ui.port", 4044) // spark ui服务端口
      .getOrCreate()
  }


  /**
    * @todo 任务运行
    * @param args 输入参数
    */
  private def runJob(args: Array[String]): Int = {
    val watcher: StopWatch = StopWatch.createStarted()
    logger.info("------------任务开始-------------main args:{}", args.mkString(","))
    val options = new OptionParser(args).getOptions
    logger.info(options.toString)
    val sparkSession = newSparkSession()
    val reader = ReaderFactory.getReader(options.getReaderName)
    val data = reader.readData(sparkSession, options)
    val writer = WriterFactory.getWrite(options.getWriterName)
    writer.writeData(sparkSession, options, data)
    watcher.stop()
    logger.info("------------任务结束-------------useTimes: {}s", watcher.getTime(TimeUnit.SECONDS))
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