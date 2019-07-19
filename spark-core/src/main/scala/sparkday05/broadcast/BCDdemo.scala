package sparkday05.broadcast

import org.apache.spark.{SparkConf, SparkContext}

/**
  * * @Author: Wmd
  * * @Date: 2019/7/10 20:18
  */
object BCDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("BCDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(Array(30,50,70,60,10))

    val bc = sc.broadcast(1 to 1000000)

    rdd1.foreach(x => println(bc.value.contains(x)))

    sc.stop()


  }

}
