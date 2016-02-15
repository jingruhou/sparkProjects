package kmust.hjr.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 开发者:刘文  Email:372065525@qq.com
  * 16/1/29   下午1:22
  * 功能描述: 计算行总和
  */
object LineSum extends App{

  val conf = new SparkConf
  /**
    * appname: 应用的名称
    * master:主机地址
    * 本地，不运行在集群值:local
    * 运行在集群:spark://s0:7077
    */
  conf.setAppName("LineSum").setMaster("spark://s0:7077")
  val jar = "/opt/workspace/bigdata/all_frame_intellij/spark-maven-scala-idea/target/spark-maven-scala-idea-1.0-SNAPSHOT.jar"
  conf.setJars(Array(jar))


  /**
    * 创建SparkContext对象
    * SparkContext 是Spark
    * 程序的所有功能的唯一入口，无论是采用Scal,Java,Python,R等
    * 同时还会负则Spark程序往Master注册程序等
    * SparkContext是整个Spark应用程序中最为至关重要的一个对象
    */
  val sc =new  SparkContext(conf)
  val textFile = sc.textFile("hdfs://s0:9000/library/wordCountBigdata")

  val lineLength = textFile.map(x => x.length)

  val sum = lineLength.reduce(_+_)
  println("求和的计算:" + sum)


  sc.stop()

  //sc.textFile("/library/wordCountBigdata",2).flatMap(line => line.split(" ")).map(w => (w,1)).reduceByKey(_ + _).foreach(println)

}
