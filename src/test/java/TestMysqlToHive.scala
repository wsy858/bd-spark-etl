import java.net.URLEncoder

import evan.wang.etl.Bootstrap
import org.junit.{Before, Test}

class TestMysqlToHive {


  @Before
  def init(): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")
    System.setProperty("debug", "true")
  }

  @Test
  def test1(): Unit = {
    val args = Array(
      "--readerName=mysql",
      "--writerName=hive",
      "--readerConfig="
        + URLEncoder.encode(
        """
          | {
          |    "driver" : "com.mysql.jdbc.Driver",
          |    "jdbcUrl" : "jdbc:mysql://localhost:3306/?zeroDateTimeBehavior=round&tinyInt1isBit=false&transformedBitIsBoolean=true",
          |    "userName" : "",
          |    "password" : "",
          |    "dbName" : "test",
          |    "tableName" : "user_info",
          |    "columns" : "id,name,gender,age,id_no,phone,address,create_time,update_time",
          |    "splitKey" : "id",
          |    "where" : "status=1"
          | }
        """.stripMargin),
      "--writerConfig=" + URLEncoder.encode(
        """
          | {
          |    "dbName" : "dw",
          |    "tableName" : "bdl_hive_user_info",
          |    "columns" : "*",
          |    "writeMode" : "append",
          |    "partition" : "dt",
          |    "partitionValue" : "20200101"
          | }
        """.stripMargin)

    )
    println(args)
    Bootstrap.main(args)
  }

}



