package Kafka_spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * * @Author: Wmd
  * * @Date: 2019/7/14 17:24
  */
object Window1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("window1")

    val ssc = new StreamingContext(conf, Seconds(4))
    //创建DStream
    val dStream = ssc.socketTextStream("hadoop102", 10000)


    /*val result = dStram.flatMap(_.split("\\W+")).map((_, 1))
      .reduceByKeyAndWindow(_ + _, Seconds(8))*/
    //也可以使用window函数创建一个带有步长的窗口DStream
    val dStream1 = dStream.window(Seconds(8))   //窗口的长度为8s
    val result = dStream1.flatMap(_.split("\\W+")).map((_,1)).reduceByKey(_+_)

    result.print
    ssc.start()
    ssc.awaitTermination()


  }

}
