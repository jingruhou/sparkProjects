package kmust.hjr.spark

import org.apache.spark.{SparkContext, SparkConf}

/**
  * 开发: 刘文  
  * 邮箱: Email:372065525@qq.com
  * 日期: 16/1/31   下午2:02
  * 功能: 
  */
object Debug{

  def main(args: Array[String]) {


      val conf = new SparkConf()
      val jar = "/opt/workspace/bigdata/all_frame_intellij/spark-maven-scala-idea/target/spark-maven-scala-idea-1.0-SNAPSHOT.jar"
      conf.setJars(Array(jar))

      //spark://s0:7077
      conf.setAppName("原理调试").setMaster("local")

      val sc = new SparkContext(conf)
      val path = "hdfs://s0:9000/library/wordcount/input/Data"
      val textFile = sc.textFile(path,2)

      textFile.foreach(x => println("textFile打印输出的内容:" + x ))
      val lines = textFile.flatMap(x => x.split(" "))
      lines.foreach(x => println("lines 打印输出的内容:" + x ))
      val word = lines.map(x => (x,1))
      word.foreach(x => println("word 打印输出的内容:" + x ))

      val wordCount = word.reduceByKey(_ + _)
      wordCount.foreach(x => println("wordCount 打印输出的内容:" + x ))
      println("程序暂停")
      Thread.sleep(1000 * 10000);
      println("程序结束")

  }


}
