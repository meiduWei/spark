package sparkday01

import org.apache.spark.{SparkConf, SparkContext}

/**
  * * @Author: Wmd
  * * @Date: 2019/7/6 20:42
  */
object Rdd {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Rdd").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val rdd1 = sc.makeRDD(Array(1,2,3,4,5))
    //val rdd2 = rdd1.map(_+1)
//    val rdd2 = rdd1.mapPartitionsWithIndex
//    rdd2.collect().foreach(println)
    sc.stop()



  }

}
