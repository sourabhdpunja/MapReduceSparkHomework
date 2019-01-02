package twitter

import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, IntegerType, StructField, StructType}

object TwitterMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.TwitterMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Twitter Follower").setMaster("local")
    val sc = new SparkContext(conf)
    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("AppName")
        .config("spark.master", "local")
        .getOrCreate()
    import spark.implicits._

		// Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================

    val customSchema = StructType(Array(
      StructField("userId", DataTypes.StringType, true),
      StructField("followerId", DataTypes.StringType, true)))

      val df = spark.read.format("csv")
        .option("delimiter",",")
        .schema(customSchema)
        .load("input/edges.csv");
      val followerCounts = df.groupBy("followerId").count()
      followerCounts.explain();
      logger.info(followerCounts.rdd.toDebugString)
      followerCounts.rdd.saveAsTextFile(args(1))
  }
}