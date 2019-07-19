package sparkday03

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.util.Random

/**
  * * @Author: Wmd
  * * @Date: 2019/7/8 19:36
  */
object KVDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize((1 to 100).map(x => new Random().nextInt(20)))
    val rdd2 = rdd1.map((_, 1))
    //    val rdd3 = rdd2.reduceByKey(_+_)
    //    val rdd3 = rdd2.reduceByKey((x,y) => (x+y+100))
    //
    //    rdd3.collect().foreach(println)

    //分区器
    val rdd3 = rdd2.partitionBy(new HashPartitioner(2))
    rdd3.glom().map(_.toList).collect.foreach(println)

    sc.stop()

  }

}
