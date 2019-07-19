package sparkday04.partitioner

import org.apache.spark.Partitioner

/**
  * * @Author: Wmd
  * * @Date: 2019/7/9 15:09
  */
class MyPartitioner(val num:Int) extends Partitioner {
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = (key.hashCode() % num).abs

  override def equals(obj: scala.Any): Boolean = super.equals(obj)

  override def hashCode(): Int = super.hashCode()

}
