package WordCountDemo

import java.io.File

import Tools.FileTools
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object WordCount {
  def main(args: Array[String]): Unit = {
    //新建配置文件环境
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setAppName("WordCount").setMaster("local")
    //建立与SparkCluster的连接
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    //读取输入的数据
    val fileRdd = sparkContext.textFile("/Users/dx/IdeaProjects/ScalaGo/README.md")
    //将它切分为一个个单词
    val words: RDD[String] = fileRdd.flatMap(line => line.split(" "))
    //将单词进行键值对转换并计数
    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
    FileTools.deleteDirectory("/Users/dx/IdeaProjects/ScalaGo/wordNum")
    counts.saveAsTextFile("/Users/dx/IdeaProjects/ScalaGo/wordNum")
    sparkContext.stop()
  }
}
