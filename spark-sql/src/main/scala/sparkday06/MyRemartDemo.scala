package sparkday06

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * * @Author: Wmd
  * * @Date: 2019/7/12 10:52
  */
object MyRemartDemo {
  def main(args: Array[String]): Unit = {
    // 测试自定义的聚合函数
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("UDFDemo1")
      .getOrCreate()
    // 注册自定义函数
    import spark.implicits._
    spark.udf.register("myRemart", new MyRemart)
    val rdd: RDD[(String, Int)] = spark.sparkContext.parallelize(List(("zs",20),("zs",40),("lisi",100),("lisi",20)))
    val df = rdd.toDF("name","age")
   // val df = spark.read.json("file://" + ClassLoader.getSystemResource("user.json").getPath)
    df.createTempView("user")
    spark.sql("select name,myRemart(age) myRemart_age from user group by name").show


  }

}
