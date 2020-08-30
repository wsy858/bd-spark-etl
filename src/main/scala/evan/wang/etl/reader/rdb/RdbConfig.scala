package evan.wang.etl.reader.rdb

import scala.beans.BeanProperty

/**
  * 数据库相关字段信息
  */
class RdbConfig {
  @BeanProperty
  var jdbcUrl: String = _
  @BeanProperty
  var userName: String = _
  @BeanProperty
  var password: String = _
  @BeanProperty
  var dbName: String = _
  @BeanProperty
  var tableName: String = _
  @BeanProperty
  var splitKey: String = _
  @BeanProperty
  var where: String = _

  override def toString = s"RdbConfig($jdbcUrl, $userName, $password, $dbName, $tableName, $splitKey, $where)"
}
