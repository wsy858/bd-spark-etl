package evan.wang.etl.reader.mysql

import evan.wang.etl.reader.rdb.RdbConfig

import scala.beans.BeanProperty

class MySqlConfig extends RdbConfig {
  @BeanProperty
  var limit: Int = _
}


