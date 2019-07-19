import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * * @Author: Wmd
  * * @Date: 2019/7/13 14:21
  */
object UpdateStateByKey {
  def main(args: Array[String]): Unit = {
    // 设置将来访问 hdfs 的使用的用户名, 否则会出现全选不够
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val conf = new SparkConf().setAppName("UpdateStateByKey").setMaster("local[4]")
    // 1. 创建SparkStreaming的入口对象: StreamingContext  参数2: 表示事件间隔
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("./update")
    val dStream = ssc.socketTextStream("hadoop102",10000)

    val result: DStream[(String, Int)] = dStream.flatMap(_.split("\\W+")).map((_,1)).updateStateByKey{
      case (seq,opt) =>Some( (seq.sum + opt.getOrElse(0)))
    }
    val resul2t: DStream[(String, Int)] = dStream.flatMap(_.split("\\W+")).map((_,1))

    result.print

    ssc.start()
    ssc.awaitTermination()




  }

}
