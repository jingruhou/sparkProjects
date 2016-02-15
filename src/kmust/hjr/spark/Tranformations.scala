package kmust.hjr.spark

import org.apache.spark.{SparkContext, SparkConf}

/**
  * 开发者:刘文  Email:372065525@qq.com
  * 16/1/22  下午8:51
  * 功能描述:main方法里面调用的每一个功能都必须是模块化的，每个模块可以用使用函数封装
  */
object Tranformations {

  def main(args: Array[String]) {
    val sc = getSparkContext
    mapTranformation(sc)
    filterTranformation(sc)
    flatMapTranformation(sc)
    groupByKeyTranformation(sc)
    reduceByKey(sc)
    joinTRanformation(sc)


      sc.stop()

  }

  /**
    * 算子操作 map
    * @param sc
    */
  def mapTranformation(sc:SparkContext): Unit ={
    println("算子操作 map")
    val nums = sc.parallelize(1 to 10) //根据集合创建RDD
    val mapped = nums.map(x => x * 2)
    mapped.collect.foreach(println)
  }

  /**
    * 算子操作 filter
    * @param sc
    */
  def filterTranformation(sc:SparkContext): Unit ={
    println("算子操作 filter")
    val nums = sc.parallelize(1 to 10) //根据集合创建RDD
    val filter = nums.filter(x => x % 2 ==0)
    filter.collect.foreach(println)
  }

  /**
    * flatMap
    * @param sc
    */
  def flatMapTranformation(sc:SparkContext): Unit ={
    println("算子操作 flatMap")
    val bigdata = Array("中 国","北 京","天 安 门")
    val mapped = sc.parallelize(bigdata)
    val flatMap = mapped.flatMap(x => x.split(" "))
    flatMap.collect.foreach(println)
  }

  /**
    * groupByKey 相同的 key 进行分组
    * @param sc
    */
  def groupByKeyTranformation(sc:SparkContext): Unit ={
    println("算子操作 groupByKey")

     val data = Array(Tuple2(100,"Spark"),Tuple2(200,"Hadoop"),Tuple2(100,"Scala"),Tuple2(120,"Hbase"))
     val dataRDD = sc.parallelize(data)
     val grouped = dataRDD.groupByKey()
     grouped.collect().foreach(println)
  }

  /**
    * reduceByKey  相同的 key 进行值累加
    * @param sc
    */
  def reduceByKey(sc:SparkContext): Unit ={
    println("算子操作 reduceByKey")
    val path = "hdfs://s0:9000/library/wordcount/input/Data"
    val lines = sc.textFile(path)  //读取文 件，并设置为一个Patitions (相当于几个Task去执行)

    val mapArray = lines.flatMap { x => x.split(" ") } //对每一行的字符串进行单词拆分并把把有行的拆分结果通过flat合并成为一个结果
    val mapMap = mapArray.map { x => (x,1) }

    val result  =  mapMap.reduceByKey(_+_) //对相同的Key进行累加
    result.collect().foreach(println)
  }

  /**
    * join 相同的 key 的值进行再次 key ,value 操作
    * @param sc
    */
  def joinTRanformation(sc:SparkContext):Unit ={
    println("算子操作 join")
    val studentNames = Array(Tuple2(1,"Spark"),Tuple2(2,"Hadoop"),Tuple2(3,"Tachyon"))
    val studentScores = Array(Tuple2(1,65),Tuple2(2,70),Tuple2(3,60))
    val names = sc.parallelize(studentNames)
    val scores = sc.parallelize(studentScores)
    val studentNameAndScore = names.join(scores)
    studentNameAndScore.collect().foreach(println)
  }

  def cogroupTranformation(sc:SparkContext):Unit={

  }

  def getSparkContext():SparkContext ={
    val conf = new SparkConf()
    conf.setAppName("Tranformations").setMaster("local")
    new SparkContext(conf)
  }
}
