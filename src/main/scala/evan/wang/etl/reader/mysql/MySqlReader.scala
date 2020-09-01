package evan.wang.etl.reader.mysql

import evan.wang.etl.reader.rdb.RdbReader

/**
  * MySQL数据源
  */
class MySqlReader extends RdbReader {

  override def quoteColumns(columns: String): String = {
    columns.trim.split(",").map("`" + _ + "`").mkString(",")
  }

}
