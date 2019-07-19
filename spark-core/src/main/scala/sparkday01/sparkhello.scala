package sparkday01

import org.apache.spark.{SparkConf, SparkContext}

/**
  * * @Author: Wmd
  * * @Date: 2019/7/6 20:27
  */
object sparkhello {
  def main(args: Array[String]): Unit = {
  // 1. 创建 SparkConf对象, 并设置 App名字
  val conf: SparkConf = new SparkConf().setAppName("sparkhello")
  // 2. 创建SparkContext对象
  val sc = new SparkContext(conf)
  // 3. 使用sc创建RDD并执行相应的transformation和action
  val wordAndCount = sc.textFile(args(0))
    .flatMap(_.split("\\W+"))
    .map((_, 1))
    .reduceByKey(_ + _)
//    .saveAsTextFile("/result")
    wordAndCount.foreach(println)
  // 4. 关闭连接
  sc.stop()


}

}
