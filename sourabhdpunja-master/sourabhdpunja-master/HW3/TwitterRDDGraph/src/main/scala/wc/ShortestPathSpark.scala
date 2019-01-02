package wc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.{Level, LogManager, Logger}

object ShortestPathSpark {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.TwitterFollowerCount <input dir> <output dir>")
      System.exit(1)
    }
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local").setAppName("Twitter Follower Count")
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================

    var adjList = sc.textFile(args(0)).map(x => (x.split(",")(0), x.split(",")(1))).groupBy(_._1).mapValues(_.map(_._2))
    var graph = sc.textFile(args(0)).map(line => {var split = line.split(","); if(split(0) == "1") (line.split(",")(0), ("true", line.split(",")(1))) else (line.split(",")(0), ("false", line.split(",")(1)))})
        .reduceByKey((x, y) => (x._1, x._2 + ":" + y._2))
    graph.persist();
    var k = "1"
    var maxCount = 100000
    var dist = graph.mapValues(adjList => (adjList._1) match { case "true" => 0 case _ => maxCount })

    var isChangedDist = true
    var sum = 0d
    while(isChangedDist) {
    dist = graph.join(dist).flatMap(x => if(x._2._2 >= maxCount) List((x._1, x._2._2)) else x._2._1._2.split(":").flatMap(node => Array((node, x._2._2 + 1), (x._1, x._2._2)))).reduceByKey((x, y) => Math.min(x, y))
    var temp = dist.values.sum()
      if(sum != temp){
        sum = temp
        isChangedDist = true
      }
      else
        isChangedDist = false
    }
    dist.saveAsTextFile(args(1))

  }
}
