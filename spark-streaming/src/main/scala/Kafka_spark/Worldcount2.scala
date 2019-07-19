package Kafka_spark

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * * @Author: Wmd
  * * @Date: 2019/7/13 11:07
  */
object Worldcount2 {
  def CreatSSC(): StreamingContext = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
    val ssc = new StreamingContext(conf, Seconds(5))
    // 偏移量保存在 checkpoint 中, 可以从上次的位置接着消费
    ssc.checkpoint("./ck-kafka")
    // kafka 参数
    //kafka参数声明
    val brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    val topic = "spark0225"
    val group = "bigdata"
   // val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"
    val kafkaParams = Map(
      "zookeeper.connect" -> "hadoop102:2181,hadoop103:2181,hadoop104:2181",
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers
    //  ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
   //   ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )
    val dStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      Set(topic))

    dStream.print()
    ssc

  }

  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./ck-kafka", CreatSSC)
    ssc.start()
    ssc.awaitTermination()
  }

}
