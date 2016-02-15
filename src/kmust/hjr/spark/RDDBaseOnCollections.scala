package kmust.hjr.spark

import org.apache.spark.{SparkContext, SparkConf}

/**
  * 开发者:刘文  Email:372065525@qq.com
  * 16/1/29   下午1:22
  * 功能描述: 计算求和
  */
object RDDBaseOnCollections extends App{

  val conf = new SparkConf
  /**
    * appname: 应用的名称
    * master:主机地址
    * 本地，不运行在集群值:local
    * 运行在集群:spark://s0:7077
    */
  conf.setAppName("RDDBaseOnCollections").setMaster("spark://s0:7077")
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


  val numbers = 1 to 100
  val rdd = sc.parallelize(numbers)
  val sum = rdd.reduce(_+_)
  println("求和的计算:" + sum)


  sc.stop()

}
