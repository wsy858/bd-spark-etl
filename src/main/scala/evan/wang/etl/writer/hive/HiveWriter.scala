package evan.wang.etl.writer.hive

import java.net.URLDecoder
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import evan.wang.etl.option.Options
import evan.wang.etl.writer.BaseWriter
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import scala.util.Random


class HiveWriter extends BaseWriter {
  private val logger: Logger = LoggerFactory.getLogger(classOf[HiveWriter])

  /**
    * 写数据
    */
  override def writeData(sparkSession: SparkSession, options: Options, data: DataFrame): Unit = {
    val hiveConfig: HiveConfig = JSON.parseObject(URLDecoder.decode(options.getWriterConfig), classOf[HiveConfig])
    val columns = buildColumns(hiveConfig, data)
    //构建新的查询列
    val revertColumns = revertColumnsFunc(data, columns)
    val partitionInfo: String = buildPartitionInfo(hiveConfig)
    val writeMode: String = hiveConfig.writeMode.toLowerCase match {
      case "append" => "INTO"
      case "overwrite" => "OVERWRITE"
    }
    //创建临时表
    val tempTable = "temp_table_" + System.nanoTime() + Random.nextInt(100)
    data.createOrReplaceTempView(tempTable)
    val sql = s"INSERT $writeMode TABLE ${hiveConfig.dbName}.${hiveConfig.tableName} $partitionInfo " +
      s"select ${revertColumns.mkString(",")} from $tempTable"
    logger.info(s"execute spark sql: $sql")
    sparkSession.sql(sql)
  }

  /**
    * 构建插入的列信息
    */
  def buildColumns(hiveConfig: HiveConfig, data: DataFrame): String = {
    var columns = hiveConfig.getColumns
    if (StringUtils.isBlank(columns) || columns.trim.equals("*") || columns.trim.equalsIgnoreCase("null")) {
      //若抽取字段列表为空则columns的值为源schema中所有字段
      columns = data.schema.fieldNames.map(f => {
        f.toLowerCase
      }).mkString(",")
    } else {
      columns = columns.trim.toLowerCase
    }
    columns
  }


  /**
    * 转换查询的列及schema， 兼容到hive存储格式为parquet的表
    *
    * @param jdbcDF  df
    * @param columns 字段列表，逗号分隔
    * @return
    */
  def revertColumnsFunc(jdbcDF: DataFrame, columns: String): ListBuffer[String] = {
    //指定查询的列
    var columnList = Array[String]()
    columnList = columns.trim.split(",").map(_.trim.toLowerCase)
    //变换Date类型后新查询的列
    var newSelectList = ListBuffer[String]()
    columnList = columns.trim.split(",")
    val schemaFieldNames = jdbcDF.schema.fieldNames.map(_.toLowerCase)
    columnList.foreach(column => {
      if (schemaFieldNames.contains(column)) {
        val schemaIndex = schemaFieldNames.indexOf(column)
        val schemaField = jdbcDF.schema.apply(schemaIndex)
        val mapping = dataTypeMapping(schemaField)
        newSelectList += mapping
      } else {
        logger.error(s"-------column: $column 字段在数据源中不存在------")
        newSelectList += s"null as $column"
      }
    })
    newSelectList
  }

  /**
    * 转换为Hive对应字段类型映射
    */
  def dataTypeMapping(field: StructField): String = {
    val fieldName = field.name.toLowerCase
    field.dataType match {
      case DateType =>
        "date_format(" + fieldName + ", 'yyyy-MM-dd') as " + fieldName
      case TimestampType =>
        "date_format(" + fieldName + ", 'yyyy-MM-dd HH:mm:ss') as " + fieldName
      case BooleanType =>
        //bit类型转为hive int类型
        "(CASE WHEN " + fieldName + " = true THEN 1 ELSE 0 END) as " + fieldName
      case DecimalType() =>
        val t: DecimalType = field.dataType.asInstanceOf[DecimalType]
        if (t.scale == 0) { //小数位为0
          "cast(" + fieldName + " as bigint) as " + fieldName
        } else {
          "cast(" + fieldName + " as DECIMAL(20, 8)) as " + fieldName
        }
      case DoubleType | FloatType =>
        "cast(" + fieldName + " as DECIMAL(20, 8)) as " + fieldName
      case StringType =>
        //换行符在HUE里展示时错乱，此处替换掉
        s"regexp_replace($fieldName, '\n|\t|\r', '') as $fieldName"
      case LongType =>
        s"cast($fieldName as bigint) as $fieldName"
      case ShortType =>
        s"cast($fieldName as short) as $fieldName"
      case _ =>
        fieldName
    }

  }

  /**
    * 构建插入分区信息
    */
  def buildPartitionInfo(hiveConfig: HiveConfig): String = {
    var partitionInfo: String = ""
    if (StringUtils.isNoneBlank(hiveConfig.getPartition)) {
      var partitionValue = hiveConfig.getPartitionValue.trim
      if (StringUtils.isBlank(partitionValue) && StringUtils.isNoneBlank(hiveConfig.getPartitionType)) {
        if ("DAY".equalsIgnoreCase(hiveConfig.getPartitionType)) {
          partitionValue = new SimpleDateFormat("yyyyMMdd").format(new Date())
        } else if ("HOUR".equalsIgnoreCase(hiveConfig.getPartitionType)) {
          partitionValue = new SimpleDateFormat("yyyyMMddHH").format(new Date())
        }
      }
      partitionInfo = s" PARTITION(${hiveConfig.getPartition}='$partitionValue')"
    }
    partitionInfo
  }


}
