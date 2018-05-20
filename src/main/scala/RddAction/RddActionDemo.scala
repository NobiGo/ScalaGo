package RddAction

import Tools.SparkContextTools
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object RddActionDemo {
  def main(args: Array[String]): Unit = {
    //   transforList()
    //    mapFunc()
    //    flatMapFunc()
    //    distinctFunc()
//    reduceFunc()
    persisFunc()
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


  def transforList(): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("转化操作").setMaster("local")
    val sparkContext = new SparkContext(conf)
    val lines: RDD[String] = sparkContext.parallelize(List("scala study", "java study"))
    println(lines.count())
    val linesSpecial = lines.filter(line => line.contains("scala"))
    val linesSpecial2 = lines.filter(line => line.contains("java"))
    println(linesSpecial.count())
    println(linesSpecial.union(linesSpecial2).count())
    val linesUnion = linesSpecial.union(linesSpecial2)
    linesUnion.saveAsTextFile("union.text")
    linesUnion.take(1).foreach(println)
  }


  def mapFunc(): Unit = {
    val sparkContext: SparkContext = SparkContextTools.connect()
    val values: RDD[Integer] = sparkContext.parallelize(List(1, 2, 3, 4, 5, 6, 7))
    val newValue: RDD[Integer] = values.map(value => value * value)
    newValue.foreach(print)
  }

  def flatMapFunc(): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("转化操作").setMaster("local")
    val sparkContext = new SparkContext(conf)
    val lines: RDD[String] = sparkContext.parallelize(List("scala study", "java study"))
    val words = lines.flatMap(line => line.split(" "))
    words.foreach(println)
  }

  def distinctFunc(): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("去重操作").setMaster("local")
    val sparkContext = new SparkContext(conf)
    val lines: RDD[String] = sparkContext.parallelize(List("scala study", "scala study"))
    val words = lines.distinct()
    words.foreach(println)
  }

  def reduceFunc(): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("去重操作").setMaster("local")
    val sparkContext = new SparkContext(conf)
    val lines: RDD[Integer] = sparkContext.parallelize(List(1, 2, 3, 4, 5, 6))
    val lineValue: Integer = lines.reduce((x, y) => x - y)
    print(lineValue)
  }

  /**
    * forEach 是不返回数据的
    */
  def forEach(): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("对RDD每个元素使用给定的函数").setMaster("local")
    val sparkContext = new SparkContext(conf)
    val lines: RDD[Integer] = sparkContext.parallelize(List(1, 2, 3, 4, 5, 6))

  }

  /**
    * 缓存数据
    */

  def persisFunc(): Unit ={
    val conf:SparkConf = new SparkConf()
    conf.setAppName("去重操作").setMaster("local")
    val sparkContext = new SparkContext(conf)
    val lines:RDD[Integer] = sparkContext.parallelize(List(1,2,3,4,5,6))
    lines.persist(StorageLevel.DISK_ONLY)
    lines.unpersist()
  }


}
