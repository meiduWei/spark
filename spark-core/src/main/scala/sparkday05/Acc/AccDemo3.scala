package sparkday05.Acc

import org.apache.spark.{SparkConf, SparkContext}

/**
  * * @Author: Wmd
  * * @Date: 2019/7/10 13:56
  */
object AccDemo3 {
  def main(args: Array[String]): Unit = {



    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20), 2)
    // 计算这些值的和, 平均值, 最大值和最小值
    val acc = new MapAcc
    sc.register(acc, "MapAcc2")
    rdd1.foreach(x => acc.add(x))

    println(acc.value)
    sc.stop()
  }

}
