package sparkday03

import org.apache.spark.{SparkConf, SparkContext}

/**
  * * @Author: Wmd
  * * @Date: 2019/7/8 21:06
  */
object Aggregate {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(0, 0, 0, 0, 0, 0, 0))
    //    val rdd2 = rdd1.aggregate(1)(_+_,_+_)  //3
    //println(rdd2)
    val rdd2 = rdd1.fold(1)(_ + _) //3
    println(rdd2)


    sc.stop()


  }

}
