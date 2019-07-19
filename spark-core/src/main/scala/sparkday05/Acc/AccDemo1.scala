package sparkday05.Acc

import org.apache.spark.{SparkConf, SparkContext}

/**
  * * @Author: Wmd
  * * @Date: 2019/7/10 10:30
  */
object AccDemo1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Array(1,2,3,4),2)

    val myAcc = new MyAcc
    //必须要注册
    sc.register(myAcc)

    rdd.foreach({
      x => myAcc.add(1)
    })
    println(myAcc.value)



  }

}
