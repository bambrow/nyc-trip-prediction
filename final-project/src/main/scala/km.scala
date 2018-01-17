package ml

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

object KM {

  def runKM(sc: SparkContext, parsed: RDD[Vector], dir: String) : KMeansModel = {

    val numClustersArray = Array(6,7,8,9,10,11,12)
    val numIterations = 200
    var bestNumClusters = 6
    var (oneWSSSE, twoWSSSE) = (Double.PositiveInfinity, Double.PositiveInfinity)
    var maxASD = Double.NegativeInfinity

    for (numClusters <- numClustersArray) {
      var totalWSSSE = 0.0
      for (i <- 1 to 5) {
        val clusters = KMeans.train(parsed, numClusters, numIterations)
        val WSSSE = clusters.computeCost(parsed)
        totalWSSSE = totalWSSSE + WSSSE
      }
      val meanMSSSE = totalWSSSE / 5
      println("NUMBER OF CLUSTERS: " + numClusters)
      println("  Within Set Sum of Squared Errors = " + meanMSSSE)
      if (twoWSSSE != Double.PositiveInfinity) {
        val curASD = twoWSSSE + meanMSSSE - 2 * oneWSSSE
        if (curASD > maxASD) {
          maxASD = curASD
          bestNumClusters = numClusters - 1
        }
      }
      twoWSSSE = oneWSSSE
      oneWSSSE = meanMSSSE
    }

    println("-------------------------------------------------------")
    println("Best Choice: NUMBER OF CLUSTERS " + bestNumClusters)
    val clusters = KMeans.train(parsed, bestNumClusters, numIterations)
    clusters.save(sc, dir)
    println("-------------------------------------------------------")
    return clusters

  }

}
