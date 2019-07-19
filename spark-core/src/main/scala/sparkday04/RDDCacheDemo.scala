package sparkday04

import org.apache.spark.{SparkConf, SparkContext}

/**
  * * @Author: Wmd
  * * @Date: 2019/7/9 20:43
  */
object RDDCacheDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("./ck1")
    val rdd1 = sc.parallelize(Array(30, 50, 20, 30))

    val rdd2 = rdd1.map(x=>{
      println("map: " + x)
      (x,1)
    })

   rdd2.cache()
    rdd2.checkpoint()
   println( rdd2.collect().toList)
    println("------------------------")
    println( rdd2.collect().toList)

  sc.stop()



  }

}
