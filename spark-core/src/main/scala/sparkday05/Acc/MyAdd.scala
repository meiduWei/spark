package sparkday05.Acc

import org.apache.spark.util.AccumulatorV2

/**
  * * @Author: Wmd
  * * @Date: 2019/7/10 10:24
  */
class MyAcc extends AccumulatorV2[Long, Long] {
  var sum: Long = 0

  override def isZero: Boolean = sum == 0

  override def copy(): AccumulatorV2[Long, Long] = {
    val Acc = new MyAcc
    sum = 0
    Acc

  }

  override def reset(): Unit = sum = 0

  override def add(v: Long): Unit = sum += v

  override def merge(other: AccumulatorV2[Long, Long]): Unit = other match {

    case o:MyAcc => sum += o.sum

  }

  override def value: Long = sum
}
