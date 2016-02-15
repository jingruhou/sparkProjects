//package com.opensourceteams.module.common.bigdata.spark.quickstart
package kmust.hjr.spark

import org.apache.spark.{SparkContext, SparkConf}

/**
  * 开发者:刘文  Email:372065525@qq.com
  * 16/1/20  下午4:30
  * 功能描述: 单词个数统计
  */
object WordCount extends App{
  /*
 * 第一步：创建Spark的配置对象SparkConf,设置Spark程序的运行时的配置信息，例如说通过setMaster来设置程序
 * 要连接的Spark集群的Master的URL,如果设置为local,则代表Spark程序在本地运行，特别适合机器配置条件非常差
 */
  val conf = new SparkConf

  val jar = "/Users/hadoop/workspace/bigdata/all_frame_intellij/spark-maven-scala-idea/target/spark-maven-scala-idea-1.0-SNAPSHOT.jar"
  conf.setJars(Array(jar))

  /**
    * appname: 应用的名称
    * master:主机地址
    * 本地，不运行在集群值:local
    * 运行在集群:spark://s0:7077
    */
  conf.setAppName("WordCountCluster9").setMaster("spark://s0:7077")

  /**
    * 创建SparkContext对象
    * SparkContext 是Spark程序的所有功能的唯一入口，无论是采用Scal,Java,Python,R等
    * 同时还会负则Spark程序往Master注册程序等
    * SparkContext是整个Spark应用程序中最为至关重要的一个对象
    */
  val sc =new  SparkContext(conf)
  /**
    * 根据具体的数据来源(HDFS、Hbse、local Fs、DB、S3等通过SparkContext来创建RDD)
    * RDD创建有三种方式:根据外部的数据来源例 如HDFS、根据Scala集合、由其它的RDD操作
    * 数据会被RDD划分成为一系列的Patitions,分配到每个Patition的数据属于一个Task的处理范畴
    */
  val path = "hdfs://s0:9000/library/wordcount/input/Data"
  val lines = sc.textFile(path)  //读取文 件，并设置为一个Patitions (相当于几个Task去执行)

  val mapArray = lines.flatMap { x => x.split(" ") } //对每一行的字符串进行单词拆分并把把有行的拆分结果通过flat合并成为一个结果
  val mapMap = mapArray.map { x => (x,1) }

  val result  =  mapMap.reduceByKey(_+_) //对相同的Key进行累加
 // result.collect().foreach(x => println("key:"+ x._1 + " value:"  + x._2))
  result.saveAsTextFile("hdfs://s0:9000/library/wordcount/output/wordcount_jar_9")

  sc.stop()

}
