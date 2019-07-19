package sparkday05

import org.apache.spark.sql.SparkSession

/**
  * Author: Wmd
  * Date: 2019/7/10 20:01
  */
object SqlDemo2 {
  def main(args: Array[String]): Unit = {
    //1.先有Session
  val spark = SparkSession.builder()
      .appName("SqlDemo2")
      .master("local[2]")
      .getOrCreate()

    //2.创建ds
    val rdd = spark.sparkContext.parallelize(List(("zs",10),("ls",20),("ww",22)))
   //一定要导入隐式转换
    import spark.implicits._
    val df = rdd.toDF("name","age")

    //3.创建临时表
    df.createTempView("user")
    //4.查询sql
    spark.sql(
      """
        |select *
        |from user
      """.stripMargin).show()

    //5.关闭
    spark.close()

  }

}
