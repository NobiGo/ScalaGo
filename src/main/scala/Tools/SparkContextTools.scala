package Tools

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author dx
  * @date 2018/5/19 下午3:21
  */
object SparkContextTools {

  def connect(): SparkContext = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local").setAppName("Scala Study")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    println("初始化SparkContext成功")
    sparkContext
  }


}
