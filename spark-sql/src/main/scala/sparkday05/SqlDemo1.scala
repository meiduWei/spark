package sparkday05


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * * @Author: Wmd
  * * @Date: 2019/7/10 19:23
  */
object SqlDemo1 {
  def main(args: Array[String]): Unit = {

    //1.先有SparkSession
    val spark: SparkSession =  SparkSession.builder()
       .appName("SqlDemo1")
       .master("local[2]")
       .getOrCreate()

    import spark.implicits._
    //2.得到df或ds
   val rdd: RDD[(String, Int)] = spark.sparkContext.parallelize(List(("zs",20),("lisi",22)))
   // case class People(name:String,age:Int)
    /*val df = rdd.map(x => {
      case (name,age) =>
    }).toDF()*/
    val df = rdd.toDF("name","age")
    //3.创建临时表
    df.createOrReplaceTempView("people")
    //4.执行sql
    spark.sql(
      """
        |select *
        |from
        |people
      """.stripMargin).show()
    //5.关闭
    spark.close()



  }

}
