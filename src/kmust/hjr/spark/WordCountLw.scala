package kmust.hjr.spark

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 开发者:刘文  Email:372065525@qq.com
  * 16/1/20  下午4:30
  * 功能描述: 单词个数统计
  */
object WordCountLw extends App{


  /*
 * 第一步：创建Spark的配置对象SparkConf,设置Spark程序的运行时的配置信息，例如说通过setMaster来设置程序
 * 要连接的Spark集群的Master的URL,如果设置为local,则代表Spark程序在本地运行，特别适合机器配置条件非常差
 */
  val conf = new SparkConf

  val jar = "/opt/workspace/bigdata/all_frame_intellij/spark-maven-scala-idea/target/spark-maven-scala-idea-1.0-SNAPSHOT.jar"
  conf.setJars(Array(jar))

  /**
    * appname: 应用的名称
    * master:主机地址
    * 本地，不运行在集群值:local
    * 运行在集群:spark://s0:7077
    */
  conf.setAppName("WordCountCluster16").setMaster("spark://s0:7077")

  /**
    * 创建SparkContext对象
    * SparkContext 是Spark
    * 程序的所有功能的唯一入口，无论是采用Scal,Java,Python,R等
    * 同时还会负则Spark程序往Master注册程序等
    * SparkContext是整个Spark应用程序中最为至关重要的一个对象
    */
  val sc =new  SparkContext(conf)


  val path = "hdfs://s0:9000/library/wordCountBigdata"

  /**
    * Read a text file from HDFS, a local file system (available on all nodes), or any
    * Hadoop-supported file system URI, and return it as an RDD of Strings.
    * 此处，从HDFS文件系统中读取文件，将返回一个字符串类型的RDD
    */
  val lines = sc.textFile(path,10)  //读取文 件，并设置为一个Patitions (相当于几个Task去执行)
  //println("打印输出:============" + lines.count())
  //lines.foreach(x => println(x))

  /**
    *  Return a new RDD by first applying a function to all elements of this
    *  RDD, and then flattening the results.
    *  首先这一个RDD中的所有元素，应运一个函数，返回一个新的RDD,然后flattening 结果
    */
  val mapArray = lines.flatMap { x => x.split(" ") } //对每一行的字符串进行单词拆分并把把有行的拆分结果通过flat合并成为一个结果
  /**
    * Return a new RDD by applying a function to all elements of this RDD.
    * RDD中的所有元素，应运一个函数
    */
  val mapMap = mapArray.map { x => (x,1) }

  /**
    * Merge the values for each key using an associative reduce function. This will also perform
    * the merging locally on each mapper before sending results to a reducer, similarly to a
    * "combiner" in MapReduce.
    * 用一个组合的 reduce 函数，合并每一个 Key 的值，在发送结果给 reducer 之前每一个局部的 mapper 执行合并操作,类似于MapReduce 的 "combiner"
    */
  val result  =  mapMap.reduceByKey(_+_,1) //对相同的Key进行累加
 // result.collect().foreach(x => println("key:"+ x._1 + " value:"  + x._2))


  /**
    * Save this RDD as a text file, using string representations of elements.
    */
  result.saveAsTextFile("hdfs://s0:9000/library/wordcount/output/wordcount_jar_20")


  Thread.sleep(1000 * 10000)
  println("结束")

  sc.stop()

}
