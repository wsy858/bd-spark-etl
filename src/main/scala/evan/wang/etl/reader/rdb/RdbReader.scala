package evan.wang.etl.reader.rdb

import java.net.URLDecoder
import java.util.Properties

import com.alibaba.fastjson.JSON
import evan.wang.etl.option.Options
import evan.wang.etl.reader.BaseReader
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

/*
 * 关系型数据库 数据源
 */
class RdbReader extends BaseReader {
  private val logger: Logger = LoggerFactory.getLogger(classOf[RdbReader])
  private val splitBatchNumber = 50000

  override def readData(sparkSession: SparkSession, options: Options): DataFrame = {
    val rdbConfig: RdbConfig = JSON.parseObject(URLDecoder.decode(options.getReaderConfig), classOf[RdbConfig])
    val properties: Properties = new Properties()
    properties.put("driver", rdbConfig.getDriver)
    properties.put("user", rdbConfig.getUserName)
    properties.put("password", rdbConfig.getPassword)
    var tableInfo = rdbConfig.getDbName + "." + rdbConfig.getTableName
    val boundAndCount = getBoundAndCount(sparkSession, rdbConfig, countSql(rdbConfig.getSplitKey, tableInfo, rdbConfig.getWhere))
    val predicates = getPredicates(rdbConfig.getSplitKey, rdbConfig.getWhere, boundAndCount._1, boundAndCount._2, boundAndCount._3)
    if (StringUtils.isNoneBlank(rdbConfig.getColumns)) {
      tableInfo = s" (select ${quoteColumns(rdbConfig.getColumns)} from $tableInfo) tx "
    }
    sparkSession.read.jdbc(rdbConfig.jdbcUrl, tableInfo, predicates, properties)
  }

  def quoteColumns(columns: String): String = {
    columns
  }

  def countSql(partitionName: String, tableName: String, where: String): String = {
    s"(select min($partitionName) as c_min, max($partitionName) as c_max, count(1) as c_count from  $tableName  where  $where ) t"
  }

  /**
    * 获取边界和总记录数
    */
  def getBoundAndCount(sparkSession: SparkSession, rdbConfig: RdbConfig, countSql: String): (Long, Long, Long) = {
    var min: Long = 0
    var max: Long = 0
    var count: Long = 0
    sparkSession.read.format("jdbc")
      .option("url", rdbConfig.getJdbcUrl)
      .option("driver", rdbConfig.getDriver)
      .option("dbtable", countSql)
      .option("user", rdbConfig.getUserName)
      .option("password", rdbConfig.getPassword)
      .option("numPartitions", 1)
      .option("queryTimeout", 60) // timeout 60s
      .load().take(1).foreach((a: Row) => {
      if (a != null) {
        min = if (a.getAs("c_min") != null) a.getAs("c_min").toString.toDouble.toLong else 0L
        max = if (a.getAs("c_max") != null) a.getAs("c_max").toString.toDouble.toLong else 0L
        count = if (a.getAs("c_count") != null) a.getAs("c_count").toString.toDouble.toLong else 0L
      }
    })
    logger.warn(s"==================================表信息统计$countSql, 结果：lowerBound: $min, upperBound: $max, totalRecord: $count")
    (min, max, count)
  }


  /**
    * 获取自定义分割查询条件
    *
    * @param partitionName 分割字段
    * @param where         查询条件
    * @param lowerBound    表最小值
    * @param upperBound    表最大值
    * @param totalRecord   表总记录数
    *                      自定义查询区间条件，一般按照主键分割
    */
  private def getPredicates(partitionName: String, where: String, lowerBound: Long, upperBound: Long, totalRecord: Long): Array[String] = {
    var buffer = ArrayBuffer[String]()
    //自定义查询批次分割条件，所有条件保存在列表中
    if (totalRecord > 0) {
      var i: Long = lowerBound //区间变量, 最开始等于最小值
      var left: Long = lowerBound //区间左侧值
      var right: Long = lowerBound //区间右侧值
      while (i <= upperBound) {
        if (i > 0 && i % splitBatchNumber == 0) {
          right = i
          buffer += s" $partitionName >= $left and $partitionName < $right and $where "
          left = right
        }
        i = i + 1
      }
      buffer += s" $partitionName >= $right and $partitionName < ${upperBound + 1} and $where "
    } else {
      buffer += s" $where " //保留原始查询条件, 目的为后面写入空的目录
    }
    buffer.toArray
  }


}
