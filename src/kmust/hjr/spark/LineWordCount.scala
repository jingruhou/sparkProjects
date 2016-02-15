package kmust.hjr.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 开发者:刘文  Email:372065525@qq.com
  * 16/1/20  下午8:21
  * 功能描述:
  */
object LineWordCount extends App {

  val conf = new SparkConf

  val jar = "/opt/workspace/bigdata/all_frame_intellij/spark-maven-scala-idea/target/spark-maven-scala-idea-1.0-SNAPSHOT.jar"
  conf.setJars(Array(jar))
  conf.setAppName("WordCountCluster").setMaster("spark://s0:7077")
  val sc = new SparkContext(conf)
  val path = "hdfs://s0:9000/library/wordcount/input/Data"
  val lines = sc.textFile(path, 2)
  //读取文 件，并设置为一个Patitions (相当于几个Task去执行)
  val lineCount = lines.map(line => (line, 1))
  //第一行变成行的内容与1构成的Tuple
  val textLines = lineCount.reduceByKey(_ + _)

  textLines.collect().foreach(x => println("输出:" + x))

}
