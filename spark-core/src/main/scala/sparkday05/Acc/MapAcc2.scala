package sparkday05.Acc

import org.apache.spark.util.AccumulatorV2

/**
  * * @Author: Wmd
  * * @Date: 2019/7/10 20:25
  */
class MapAcc2 extends AccumulatorV2[Int, Map[String, Double]] {
  var map = Map[String, Double]()
  var count = 0

  override def isZero: Boolean = map.isEmpty && count == 0

  override def copy(): AccumulatorV2[Int, Map[String, Double]] = {
    println("copy")
    val acc = new MapAcc2
    acc.map = Map[String, Double]() //重新赋值
    count = 0
    acc
  }

  override def reset(): Unit = {
    map = Map[String, Double]()
    count = 0
  }

  override def add(v: Int): Unit = {
    map += "sum" -> (map.getOrElse("sum", 0d) + v)
    map += "max" -> map.getOrElse("max", Int.MinValue.toDouble).max(v)
    map += "min" -> map.getOrElse("min", Int.MaxValue.toDouble).min(v)
    count += 1
  }

  override def merge(other: AccumulatorV2[Int, Map[String, Double]]): Unit = {
    other match {
      case o: MapAcc2 =>
        this.map += "sum" -> (this.map.getOrElse("sum", 0d) + o.map.getOrElse("sum", 0d))
        this.map += "max" -> this.map.getOrElse("max", Int.MinValue.toDouble).max(o.map.getOrElse("max", 0d))
        this.map += "min" -> this.map.getOrElse("min", Int.MaxValue.toDouble).min(o.map.getOrElse("min", 0d))
        count += 1
    }

  }

  override def value: Map[String, Double] = {
    //将均值也加到字段中
    map += "avg" -> (map.getOrElse("sum", 0d) / count)
    map
  }
}
