package ml

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import helper.Helper._
import helper.TaxiHelper._
import helper.BikeHelper._

object DT {

  def runDT(sc: SparkContext, args: Array[String], weatherData: RDD[(String, (Double, Double, Double, Double))]) {

    if (args(4).toLowerCase == "taxi") {
      if (args(6).toLowerCase == "training") {
        val data = getJoinedTaxiData(sc, weatherData, args(1), args(3)).cache()
        runTrainingDT(sc, args(2), data)
      } else if (args(6).toLowerCase == "test") {
        val data = getJoinedTaxiDataKM(sc, weatherData, args(1), args(3)).cache()
        runTestDT(sc, args(2), data)
      } else {
        val data = getJoinedTaxiDataPred(sc, weatherData, args(1), args(3)).cache()
        runPredDT(sc, args(2), data)
      }
    } else {
      if (args(6).toLowerCase == "training") {
        val data = getJoinedBikeData(sc, weatherData, args(1), args(3)).cache()
        runTrainingDT(sc, args(2), data)
      } else if (args(6).toLowerCase == "test") {
        val data = getJoinedBikeDataKM(sc, weatherData, args(1), args(3)).cache()
        runTestDT(sc, args(2), data)
      } else {
        val data = getJoinedBikeDataPred(sc, weatherData, args(1), args(3)).cache()
        runPredDT(sc, args(2), data)
      }
    }

  }

  def runTrainingDT(sc: SparkContext, dir: String, data: RDD[Array[Double]]) {

    val (parsedTraining, parsedTest) = getTrainingAndTest(data)

    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "variance"
    val maxDepthArray = Array(2,3,4,5,6,7,8,10,12,16)
    val maxBinsArray = Array(8,16,24,32,48,64,96,128,256,512)

    var (bestMaxDepth, bestMaxBins, currentMinMSE) = (2,8,Double.PositiveInfinity)
    var bestModel = None : Option[DecisionTreeModel]

    for (maxDepth <- maxDepthArray) {
      for (maxBins <- maxBinsArray) {

        val model = DecisionTree.trainRegressor(parsedTraining, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

        val trainMSE = getMSE(parsedTraining, model)
        // println("Learned regression tree model:\n" + model.toDebugString)
        println("MAX DEPTH: " + maxDepth + ";   MAX BINS: " + maxBins)
        println("Training Data Mean Squared Error = " + trainMSE)

        val testMSE = getMSE(parsedTest, model)
        println("Test Data Mean Squared Error = " + testMSE)

        if (testMSE < currentMinMSE) {
          currentMinMSE = testMSE
          bestMaxDepth = maxDepth
          bestMaxBins = maxBins
          bestModel = Some(model)
        }

      }
    }

    println("-------------------------------------------------------")
    println("Best Choice: MAX DEPTH " + bestMaxDepth + ", MAX BINS " + bestMaxBins)
    println("  with MSE " + currentMinMSE)

    bestModel match {
      case None => Console.err.println("Error! No model is saved!")
      case Some(value) => value.save(sc, dir)
    }

    println("Optimization complete. Exit.")
    println("-------------------------------------------------------")

  }

  def runTestDT(sc: SparkContext, dir: String, data: RDD[Array[Double]]) {

    val model = DecisionTreeModel.load(sc, dir)
    val parsed = parseData(data)

    println("Learned regression tree model:\n" + model.toDebugString)
    val MSE = getMSE(parsed, model)
    println("-------------------------------------------------------")
    println("All Data Mean Squared Error = " + MSE)
    val predicted = parsed.map { point =>
      val prediction = model.predict(point.features)
      (prediction, point.label - prediction)
    }
    val combined = data.zip(predicted).map{ case (v1,(v2,v3)) => v1 ++ Array(v2,v3) }
    combined.map(items => items.mkString(",")).saveAsTextFile(dir + "_out_test")
    println("Save complete!")
    println("-------------------------------------------------------")

  }

  def runPredDT(sc: SparkContext, dir: String, data: RDD[Array[Double]]) {

    val model = DecisionTreeModel.load(sc, dir)
    val vec = data.map(items => Vectors.dense(items))
    val pred = model.predict(vec)
    println("-------------------------------------------------------")
    println("Prediction complete!")
    val combined = data.zip(pred).map{ case (v1,v2) => v1 ++ Array(v2) }
    combined.map(items => items.mkString(",")).saveAsTextFile(dir + "_out_prediction")
    println("Save complete!")
    println("-------------------------------------------------------")

  }

}
