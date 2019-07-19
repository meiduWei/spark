package sparkday03

import org.apache.spark.{SparkConf, SparkContext}

/**
  * * @Author: Wmd
  * * @Date: 2019/7/8 19:22
  */
object GroupBy {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(30, 50, 7, 60, 1, 20))

    val rdd2 = rdd1.groupBy(x => x % 2)   //按
    rdd2.map {
      case (x, it) => (x, it.size) //偏函数的使用
    }.collect().foreach(println)


    sc.stop()

  }

}
