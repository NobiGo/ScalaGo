package WordCountDemo

import org.apache.spark.{SparkConf, SparkContext}


object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf:SparkConf = new SparkConf()
    sparkConf.setAppName("WordCount").setMaster("local")
    val sparkContext:SparkContext = new SparkContext(sparkConf)
    val fileRdd  = sparkContext.textFile("/Users/dx/IdeaProjects/ScalaGo/README.md")
    fileRdd.saveAsTextFile("/Users/dx/IdeaProjects/ScalaGo/out")
    print(fileRdd.count())
    sparkContext.stop()
  }
}
