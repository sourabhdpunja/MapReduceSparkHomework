package PageRank

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager

object PageRankMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    if (args.length != 2) {
      logger.error("Usage:\nwc.PageRankMain <output dir> <k>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Page Rank").setMaster("local")
    val sc = new SparkContext(conf)
    var temp2Map = scala.collection.Map[Int, Double]()
    var delta:Double = 0.0

    val k = args(1).toInt;

    def funcForAdjacency(x:Int) = {
      var firstVertice = x;
      var rowAdjacencyList:  List[(Int, Int)] = Nil
      for(a <- 1 until k){
        var y = (firstVertice, firstVertice+1)
        firstVertice = firstVertice + 1
        val appendNilList: List[(Int, Int)] = y :: Nil
        rowAdjacencyList =  rowAdjacencyList ::: appendNilList
      }
      rowAdjacencyList = rowAdjacencyList ::: List(((x + k - 1), 0))
      rowAdjacencyList
    }


    val verticesOfFirstColumn = (1 to k).map(x => (x*k - (k -1)))
    val adjacencyList = verticesOfFirstColumn.map(funcForAdjacency).flatMap(x => x).toList
    val listOfVertices = List.range(1, ((k*k)+1))
    val pageRanksWithoutDummy = listOfVertices.map(x => (x, (1 / (k*k).toDouble)))
    val pageRanks = pageRanksWithoutDummy ::: List((0, 0.0))
    val rddAdjList = sc.parallelize(adjacencyList)
    rddAdjList.persist()
    var rddPageRanks = sc.parallelize(pageRanks)

    for (i <- 1 to 10) {
      val joinedRdd = rddAdjList.join(rddPageRanks)
      val joinTripletRdd = joinedRdd.map(x => (x._1, x._2._1, x._2._2))
      val temp = joinTripletRdd.map(x => (x._2, x._3))
      val temp2 = temp.reduceByKey(_+_)
      val prOfZeroVertice = temp2.filter(x => (x._1 == 0)).first()._2
      delta = prOfZeroVertice/(k*k)
      val temp2WithoutZeroVertice = temp2.filter(x => (x._1 != 0))
      val temp2WithDelta = temp2WithoutZeroVertice.map(x => (x._1, (x._2 + delta)))
      println("temp2withdelta")
      val temp2Tuple = temp2WithDelta.map(x => (x._1, x._2))
      temp2Map = temp2Tuple.collectAsMap()
      rddPageRanks = rddPageRanks.map(funy)
//      rddPageRanks.collect().foreach(println)
      val sumOfRanks = rddPageRanks.map(_._2).sum()
      logger.info(rddPageRanks.toDebugString)
//      println(sumOfRanks)
    }
//    logger.info(rddPageRanks.toDebugString)

    def funy(x: (Int, Double)) = {
      val getMapValue = temp2Map.get(x._1);
      if (getMapValue != None){
        val tupleForRank = (x._1, getMapValue.get)
        tupleForRank
      } else if (x._1 == 0) {
        x
      }
      else {
          (x._1, delta)
      }
    }
    val listOfPrValues = rddPageRanks.collect().sortWith((x, y) => x._2 > y._2).take(100)
    sc.parallelize(listOfPrValues).saveAsTextFile(args(0))
  }
}
