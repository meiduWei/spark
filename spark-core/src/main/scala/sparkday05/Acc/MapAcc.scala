package sparkday05.Acc

import org.apache.spark.util.AccumulatorV2

/**
  * * @Author: Wmd
  * * @Date: 2019/7/10 11:18
  */
class MapAcc extends AccumulatorV2[Int, Map[String, Double]] {
  //  new Map(Int,(String,Double))
  private var map = Map[String, Double]()
  var count = 0

  override def isZero: Boolean = map.isEmpty


  override def copy(): AccumulatorV2[Int, Map[String, Double]] = {
    val acc = new MapAcc
    acc.map = Map[String, Double]()
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
      case o: MapAcc =>
        this.map += "sum" -> (map.getOrElse("sum", 0d) + o.map.getOrElse("sum", 0d))
        this.map += "max" -> map.getOrElse("max", Int.MinValue.toDouble).max(o.map.getOrElse("max", Int.MinValue.toDouble))
        this.map += "min" -> map.getOrElse("min", Int.MaxValue.toDouble).min(o.map.getOrElse("min", Int.MaxValue.toDouble))
        count += o.count
      case _ => throw new UnsupportedOperationException
    }
  }

  override def value: Map[String, Double] ={

    map += "avg" -> map.getOrElse("sum",0d) /count
    map

  }
}
