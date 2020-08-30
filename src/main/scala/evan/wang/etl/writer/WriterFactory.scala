package evan.wang.etl.writer

import evan.wang.etl.writer.hive.HiveWriter

import scala.collection.mutable

object WriterFactory {

  //注册writes
  private val WRITERS: mutable.Map[String, Class[_ <: BaseWriter]] = mutable.Map()

  //注册具体的write
  registerLauncher("hive",classOf[HiveWriter])

  /**
    * @todo 动态注册write类
    * @param name write名称
    * @param cls  write类
    */
  def registerLauncher(name: String, cls: Class[_ <: BaseWriter]): Unit = {
    WRITERS += (name -> cls)
  }

  /**
    * @todo 获取write类实例
    * @param name write名称
    * @return write实例
    */
  def getWrite(name: String): BaseWriter = {
    val cls = WRITERS(name)
    try {
      if (null != cls) {
        val write = cls.newInstance
        write.setName(name)
        return write
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    null
  }


}
