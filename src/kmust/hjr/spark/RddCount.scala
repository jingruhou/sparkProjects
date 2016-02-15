package kmust.hjr.spark

import org.apache.spark.{SparkContext, SparkConf}

/**
  * 开发: 刘文  
  * 邮箱: Email:372065525@qq.com
  * 日期: 16/2/2   上午10:47
  * 功能: 
  */
object RddCount extends App{
  /**
    * 指定本地hadoop 根目录，远程调试用到
    * 不设置环境变量HADOOP_HOME 时，会报错，所以需要设置此目录
    */
  System.setProperty("hadoop.home.dir", "/opt/modules/bigdata/hadoop/hadoop-2.6.0")
  //指定本地hadoop 根目录，远程调试用到()

  val conf = new SparkConf()
  val jar = "/opt/workspace/bigdata/all_frame_intellij/spark-maven-scala-idea/target/spark-maven-scala-idea-1.0-SNAPSHOT.jar"
  conf.setJars(Array(jar))

  //spark://s0:7077  local
  conf.setAppName("原理调试").setMaster("spark://s0:7077")

  val sc = new SparkContext(conf)
  var path = "/opt/workspace/gitoschina/opensourceteams/data/input/wordCount"
  path = "hdfs://s0:9000/library/wordcount/input/Data"
  val textFile = sc.textFile(path,10)
  val count = textFile.count()
  println("统计行数:" + count)

  Thread.sleep(1000 * 10000)
  println("结束")
}
