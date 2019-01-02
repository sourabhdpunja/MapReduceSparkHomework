package twitter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.execution
import org.apache.spark.broadcast.Broadcast

object FollowerNew {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ntw.FolloweCount <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Follower Count")
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================

    val followerSchema = StructType(Array(
      StructField("followedBy", StringType, true),
      StructField("userId", StringType, true)
    ))


    val spark = SparkSession
      .builder()
      .appName("Follower count")
      .getOrCreate()

    import spark.implicits._


    // reads the edges.csv file and uses followerSchema to covert to Dataset
    val df = spark.read.format("csv").schema(followerSchema).load(args(0))


    val ds = df.filter($"userId" < 100 && $"followedBy" < 100)

    val path2 = ds.as("df1").join((ds.as("df2")), $"df1.userId" === $"df2.followedBy")
      .select($"df1.followedBy".alias("start"), $"df1.userId".alias("mid"), $"df2.userId".alias("end"))

    println(path2.queryExecution.sparkPlan)
    // path2.explain()

    // val count = ds.join(path2, $"followedBy" === $"end" && $"userId" === $"start").count()

    // logger.info("***")
    // logger.info(count/3)
  }
}