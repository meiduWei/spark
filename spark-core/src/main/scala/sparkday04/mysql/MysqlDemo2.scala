package sparkday04.mysql

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * * @Author: Wmd
  * * @Date: 2019/7/9 20:15
  */
object MysqlDemo2 {
  def main(args: Array[String]): Unit = {

    //向mysql中插入数据
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20))

    //定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val userName = "root"
    val passWd = "000000"

    rdd1.foreachPartition(it => {
      // 建立jdbc连接
      Class.forName(driver)
      val conn: Connection = DriverManager.getConnection(url, userName, passWd)

      val sql = "insert into test values(?)"
      it.foreach(x => {x
        val ps: PreparedStatement = conn.prepareStatement(sql)
        ps.setInt(1, x)
        ps.executeUpdate()
        ps.close()
      })
      conn.close()
    })

    sc.stop()

  }

}
