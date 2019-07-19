package sparkday03

import org.apache.spark.{SparkConf, SparkContext}

/**
  * * @Author: Wmd
  * * @Date: 2019/7/8 15:33
  */
object Practice {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Practice ").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile(ClassLoader.getSystemResource("agent.log").getPath)
    val rdd2 = rdd1.map(line => {
      val splis = line.split("\\W+")
      ((splis(1), splis(4)), 1)

    })
    val rdd3 = rdd2.reduceByKey(_ + _)
    //val rdd4 = rdd3.map(x =>(x._1._1,(x._1._2,x._2)))
    val rdd4 = rdd3.map({
      case ((pId, adsId), count) => (pId, (adsId, count))
    })
    val rdd5 = rdd4.groupByKey()
    val rdd6 = rdd5.mapValues(
      adsCount => {
        adsCount.toList.sortBy(_._2).take(3)
      }
    )
    rdd6.collect().foreach(println)
    sc.stop()
  }

}
