package com.atgui

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object Flink_helloworld {
  def main(args: Array[String]): Unit = {
    //创建批处理的执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //引入文件路径，读取文件
    val inpath ="E:\\Ideaproject\\flink-3\\src\\main\\resources\\test.txt"
    val inputdataset: DataSet[String] = env.readTextFile(inpath)
    //基于批处理的dataset进行批处理

    val result: AggregateDataSet[(String, Int)] = inputdataset.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    Thread.sleep(300)

    result.print()



  }

}
