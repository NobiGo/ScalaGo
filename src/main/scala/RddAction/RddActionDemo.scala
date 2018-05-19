package RddAction

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RddActionDemo {
  def main(args: Array[String]): Unit = {
   transforList()
  }

  def actionRdd = {
    val conf: SparkConf = new SparkConf()
    conf.setMaster("local").setAppName("RddAction")
    val sparkContext: SparkContext = new SparkContext(conf)
    val fileRdd: RDD[String] = sparkContext.textFile("/Users/dx/IdeaProjects/ScalaGo/README.md")
    fileRdd.persist()
    print("总的行数为" + fileRdd.count())
    val lines: RDD[String] = fileRdd.filter(line => line.contains("study"))
    println("符合条件的行数为：" + lines.count())
  }


  def transforList(): Unit ={
    val conf:SparkConf = new SparkConf()
    conf.setAppName("转化操作").setMaster("local")
    val sparkContext = new SparkContext(conf)
    val lines:RDD[String] = sparkContext.parallelize(List("scala study","java study"))
    println(lines.count())
    val linesSpecial = lines.filter(line=>line.contains("scala"))
    val linesSpecial2 = lines.filter(line=>line.contains("java"))
    println(linesSpecial.count())
    println(linesSpecial.union(linesSpecial2).count())
    val linesUnion = linesSpecial.union(linesSpecial2)
    linesUnion.saveAsTextFile("union.text")
    linesUnion.take(1).foreach(println)
  }
}
