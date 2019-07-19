package sparkday04.mysql

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * * @Author: Wmd
  * * @Date: 2019/7/9 20:06
  */
object MysqlDemo1 {
  def main(args: Array[String]): Unit = {

    //向mysql中查询数据
    val conf = new SparkConf().setAppName("MysqlDemo1").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val userName = "root"
    val passWd = "000000"

    val rdd = new JdbcRDD[(Int, String)](
      sc,
      () => {
        Class.forName(driver)
        DriverManager.getConnection(url, userName, passWd)
      },
      "select id from test where ? <= id and id <= ?",
      10,
      100,
      3,
      result => (result.getInt(1), "abc")
    )
    rdd.collect().foreach(println)
    sc.stop()



  }

}
