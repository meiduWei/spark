package sparkday03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * * @Author: Wmd
  * * @Date: 2019/7/8 20:54
  */
object Filter {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("GlomDemo").setMaster("local[2]")

    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(1 to 10)

    rdd1.filter(_ > 5).collect().foreach(println)





  }

}
