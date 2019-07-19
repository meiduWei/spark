package sparkday03

import org.apache.spark.{SparkConf, SparkContext}

/**
  * * @Author: Wmd
  * * @Date: 2019/7/8 19:44
  */
object KVDemo2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
    //先求每个分区内每个value的最大值,再将两个分区的最大值求和  aggregateByKey
    //val rdd2 = rdd1.aggregateByKey(Int.MinValue)((last,ele) =>last.max(ele),_+_)
    //  rdd2.collect().foreach(println)

    /* val rdd2 = rdd1.reduceByKey(_+_)
     rdd2.collect().foreach(println)*/

    // val rdd2 = rdd1.foldByKey(0)(_+_)

    //val rdd2 = rdd1.combineByKey(x=>x,(last:Int,ele:Int) => (last.max(ele)),(m1:Int,m2:Int)=>( m1+m2))

    // rdd2.collect().foreach(println)

    //同时将最大值和最小值输出
    val rdd2 = rdd1.combineByKey(
      x => (x, x),
      (x: (Int, Int), ele) => (x._1.max(ele), x._2.min(ele)),
      (mm1: (Int, Int), mm2: (Int, Int)) => (mm1._1 + mm2._1, mm1._2 + mm2._2)
    )
    rdd2.collect().foreach(println)





    //val rdd2 = rdd1.mapPartitions(it => it)
    //println(rdd2.getNumPartitions)   //获取分区数
    //rdd2.collect().foreach(println)
    //使用偏函数进行处理
    /*val rdd2 = rdd1.map({
      case(k,v) =>  (k,v + 100)
    })*/
    //val rdd2 = rdd1.map( kv => (kv._1,kv._2+100) )
    // rdd2.collect().foreach(println)


  }

}
