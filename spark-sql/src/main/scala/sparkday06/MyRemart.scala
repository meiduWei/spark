package sparkday06

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * * @Author: Wmd
  * * @Date: 2019/7/12 9:56
  */
class MyRemart extends UserDefinedAggregateFunction {

  //输入值的类型
  override def inputSchema: StructType = {
    StructType(StructField("input", DoubleType) :: Nil)
  }

  //一个缓冲区.两个字段
  override def bufferSchema: StructType = {
    StructType(StructField("sum", DoubleType) :: StructField("count", LongType) :: Nil)
  }

  //返回值类型
  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  //初始化缓冲区
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //给缓冲区的两个字段赋初值
    buffer(0) = 0d
    buffer(1) = 0L

  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)){
      buffer(0) = buffer.getDouble(0) +input.getDouble(0)
      //每次增加1即可
      buffer(1)  = buffer.getLong(1) + 1
    }

  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (!buffer1.isNullAt(0)){
      buffer1(0) = buffer1.getDouble(0) +buffer2.getDouble(0)
      buffer1(1)  = buffer1.getLong(1) + buffer2.getLong(1)
    }


  }

  override def evaluate(buffer: Row) = {
     s"sum: ${buffer.getDouble(0)} avg: ${buffer.getDouble(0) / buffer.getLong(1)} count: ${buffer.getLong(1)}"
  }



}
