package sparkday03

import org.apache.spark.{SparkConf, SparkContext}

/**
  * * @Author: Wmd
  * * @Date: 2019/7/8 20:49
  */
object SortBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(30, 50, 70, 1))


    rdd1.map(x => {
      println(x)
      x
    })sortBy(x => x)






  }

}
