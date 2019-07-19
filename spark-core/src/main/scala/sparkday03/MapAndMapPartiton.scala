package sparkday03

import org.apache.spark.{SparkConf, SparkContext}

/**
  * * @Author: Wmd
  * * @Date: 2019/7/8 20:01
  */
object MapAndMapPartiton {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(30, 50, 7, 60, 1, 20))

    rdd1.mapPartitionsWithIndex((index,intor) => intor.map( intor => (index,intor))).collect().foreach(println)



  }
}
