package PageRank

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object PageRankMainNew {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    val conf = new SparkConf().setAppName("Page Rank").setMaster("local")
    val sc = new SparkContext(conf)

    if (args.length != 2) {
      logger.error("Usage:\nwc.PageRankMain <output dir> <k>")
      System.exit(1)
    }

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("AppName")
        .config("spark.master", "local")
        .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    var temp2Map = scala.collection.Map[Int, Double]()
    var delta: Double = 0.0f
    val k = args(1).toInt;

    def funcForAdjacency(x: Int) = {
      var firstVertice = x;
      var rowAdjacencyList: List[(Int, Int)] = Nil
      for (a <- 1 until k) {
        var y = (firstVertice, firstVertice + 1)
        firstVertice = firstVertice + 1
        val appendNilList: List[(Int, Int)] = y :: Nil
        rowAdjacencyList = rowAdjacencyList ::: appendNilList
      }
      rowAdjacencyList = rowAdjacencyList ::: List(((x + k - 1), 0))
      rowAdjacencyList
    }

    val verticesOfFirstColumn = (1 to k).map(x => (x * k - (k - 1)))
    val adjacencyList = verticesOfFirstColumn.map(funcForAdjacency).flatMap(x => x).toList
    val listOfVertices = List.range(1, ((k * k) + 1))
    val pageRanksWithoutDummy = listOfVertices.map(x => (x, (1 / (k * k).floatValue())))
    val pageRanks = pageRanksWithoutDummy ::: List((0, 0f))
    val dfAdjList = adjacencyList.toDF("v1", "v2")
    dfAdjList.persist()
    var dfPageRanks = pageRanks.toDF("v1", "pr")
    for (i <- 1 to 10) {
      val joinedDf = dfAdjList.join(dfPageRanks, "v1")
      val tempDf = joinedDf.select("v2", "pr")
      val temp2 = tempDf.groupBy("v2").sum("pr").toDF("v2", "pr")
      val prOfZeroVertice = temp2.filter($"v2" === "0").select("pr").first().getDouble(0).toFloat
      delta = (prOfZeroVertice / (k * k))
      val temp2WithoutZeroVertice = temp2.filter($"v2" !== 0)
      val temp2WithDelta = temp2WithoutZeroVertice.withColumn("pr", col("pr").cast("float") + delta).toDF("v2", "pr")
      val joinedDfPageRanks = dfPageRanks.as("pagerank").join(temp2WithDelta.as("temp2"), $"v1" === $"v2", "leftouter")
      val dfPageRanksNew = joinedDfPageRanks.toDF("v1", "v1pr", "v2", "v2pr")
      dfPageRanks = dfPageRanksNew.select($"v1", when($"v2pr".isNull, delta).otherwise($"v2pr").alias("pr").cast("float"))
      val prSum = dfPageRanks.select(col("pr")).rdd.map(_(0).asInstanceOf[Float]).reduce(_+_)
      println(prSum)
    }
    logger.info(dfPageRanks.explain())
    val listOfPrValues = dfPageRanks.orderBy(desc("pr")).take(100)
    sc.parallelize(listOfPrValues).saveAsTextFile(args(0))
  }
}