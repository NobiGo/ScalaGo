package RddAction

import Tools.SparkContextTools
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * @author dx
  * @date 2018/5/19 下午3:14
  *
  * 1. lamba表达式
  * 2. 局部函数
  *
  */
object ParaAction {
  def main(args: Array[String]): Unit = {
//    funcLambda()
    funcFunc()
  }

  //lambda表达式
  def funcLambda(): Unit = {
    val sparkContext: SparkContext = SparkContextTools.connect()
    val file: RDD[String] = sparkContext.textFile("/Users/dx/IdeaProjects/ScalaGo/README.md")
    val lines: RDD[String] = file.filter(line => line.contains("Scala"))
    print(lines.count())
    sparkContext.stop()
  }

  //局部函数
  def containsScala(args: String): Boolean = {
    args.contains("Go")
  }

  def funcFunc(): Unit ={
    val sparkContext: SparkContext = SparkContextTools.connect()
    val file: RDD[String] = sparkContext.textFile("/Users/dx/IdeaProjects/ScalaGo/README.md")
    val lines: RDD[String] = file.filter(containsScala)
    print(lines.count())
    sparkContext.stop()
  }
}
