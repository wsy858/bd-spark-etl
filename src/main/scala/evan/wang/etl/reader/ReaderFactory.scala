package evan.wang.etl.reader

import evan.wang.etl.reader.mysql.MySqlReader

import scala.collection.mutable

object ReaderFactory {

  //注册readers
  private val READERS: mutable.Map[String, Class[_ <: BaseReader]] = mutable.Map()

  //注册具体的reader
  registerLauncher("mysql",classOf[MySqlReader])

  /**
    * @todo 动态注册reader类
    * @param name reader名称
    * @param cls  reader类
    */
  def registerLauncher(name: String, cls: Class[_ <: BaseReader]): Unit = {
    READERS += (name -> cls)
  }

  /**
    * @todo 获取reader类实例
    * @param name reader名称
    * @return reader实例
    */
  def getReader(name: String): BaseReader = {
    val cls = READERS(name)
    try {
      if (null != cls) {
        val reader = cls.newInstance
        reader.setName(name)
        return reader
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    null
  }


}
