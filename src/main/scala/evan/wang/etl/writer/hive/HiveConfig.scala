package evan.wang.etl.writer.hive

import scala.beans.BeanProperty

class HiveConfig {
  //插入库名称
  @BeanProperty
  var dbName:String = _
  //插入表名称
  @BeanProperty
  var tableName: String = _
  //插入表字段，逗号分隔
  @BeanProperty
  var columns: String = _
  //写入模式，append：追加，overwrite：覆盖
  @BeanProperty
  var writeMode: String = _
  //分区字段名称
  @BeanProperty
  var partition: String = _
  //分区类型 (DAY、HOUR)
  //DAY：天分区，分区示例：dt=20200101
  //HOUR：小时分区，分区示例：dt=2020010112
  @BeanProperty
  var partitionType:String = _
  //分区值（可选）
  @BeanProperty
  var partitionValue:String =_

}
