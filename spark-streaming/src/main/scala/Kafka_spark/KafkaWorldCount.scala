package Kafka_spark

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * * @Author: Wmd
  * * @Date: 2019/7/13 9:28
  */
object KafkaWorldCount1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaWorldCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    val brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    val topic = "spark0225"
    val group = "bigdata"
   // val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"

    val kafkaParams = Map(
      "zookeeper.connect" -> "hadoop102:2181,hadoop103:2181,hadoop104:2181",
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers
      //反序列化器
     // ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
     // ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )
    val dStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      Set(topic))

    dStream.map((_._2)).flatMap(_.split("\\W+")).map((_, 1)).reduceByKey(_ + _).print

    ssc.start()
    ssc.awaitTermination()
  }

}
