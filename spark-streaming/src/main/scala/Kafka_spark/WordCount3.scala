package Kafka_spark

import kafka.common.TopicAndPartition
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.kafka.KafkaCluster

/**
  * * @Author: Wmd
  * * @Date: 2019/7/14 16:25
  */
object WordCount3 {
  val brokers = "hadoop201:9092,hadoop202:9092,hadoop203:9092"
  val group = "bigdata"
  val topic = "spark0225"
  val kafkaParams = Map(
    "zookeeper.connect" -> "hadoop201:2181,hadoop202:2181,hadoop203:2181",
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
    ConsumerConfig.GROUP_ID_CONFIG -> group
  )
  // 用来处理offsets的KafkaCluster
  private val kafkaCluster = new KafkaCluster(kafkaParams)

  //读取到上次消费到的offsets
  def readOffsets ={
    var result = Map[TopicAndPartition,Long]()

    val topicAndPartitionSetEither = kafkaCluster.getPartitions(Set(topic))
    topicAndPartitionSetEither match {
      case Right(topicAndPartitions) =>
        //获取每个topic和每个分区的offset的信息
        val topicAndPartitionOffsetMapEither = kafkaCluster.getConsumerOffsets(group,topicAndPartitions)
        if (topicAndPartitionOffsetMapEither.isRight){
          result ++= topicAndPartitionOffsetMapEither.right.get
        }else{//有topic和分区, 但是没有相关的偏移量
          // 把每个topic的每个分区的偏移都设置为0
          topicAndPartitions.foreach( topicAndPartition =>{
            result += topicAndPartition -> 0
          })


        }
      case  _ =>
    }

    result

  }






}
