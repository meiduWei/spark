package sparkday04.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * * @Author: Wmd
  * * @Date: 2019/7/9 15:12
  */
object MypartitonerDemo1 {
  def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
     val sc = new SparkContext(conf)
     val rdd1: RDD[String] = sc.textFile("D:\\tmp\\input")

     rdd1.collect()
     sc.stop()



  }

}
