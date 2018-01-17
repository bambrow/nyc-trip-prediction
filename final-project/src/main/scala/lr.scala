package ml

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import helper.Helper._
import helper.TaxiHelper._
import helper.BikeHelper._

object LR {

  def runLR(sc: SparkContext, args: Array[String], weatherData: RDD[(String, (Double, Double, Double, Double))]) {

    if (args(4).toLowerCase == "taxi") {
      if (args(6).toLowerCase == "training") {
        val data = getJoinedTaxiData(sc, weatherData, args(1), args(3)).cache()
        runTrainingLR(sc, args(2), data)
      } else if (args(6).toLowerCase == "test") {
        val data = getJoinedTaxiDataKM(sc, weatherData, args(1), args(3)).cache()
        runTestLR(sc, args(2), data)
      } else {
        val data = getJoinedTaxiDataPred(sc, weatherData, args(1), args(3)).cache()
        runPredLR(sc, args(2), data)
      }
    } else {
      if (args(6).toLowerCase == "training") {
        val data = getJoinedBikeData(sc, weatherData, args(1), args(3)).cache()
        runTrainingLR(sc, args(2), data)
      } else if (args(6).toLowerCase == "test") {
        val data = getJoinedBikeDataKM(sc, weatherData, args(1), args(3)).cache()
        runTestLR(sc, args(2), data)
      } else {
        val data = getJoinedTaxiDataPred(sc, weatherData, args(1), args(3)).cache()
        runPredLR(sc, args(2), data)
      }
    }

  }

  def runTrainingLR(sc: SparkContext, dir: String, data: RDD[Array[Double]]) {

    val (parsedTraining, parsedTest) = getTrainingAndTest(data)

    val numIterationsArray = Array(25,50,75,100,250,500)
    val stepSizeArray = Array(0.000001,0.0000001,0.00000001,0.000000001,0.0000000001)

    var (bestNumIterations, bestStepSize, currentMinMSE) = (25,0.01,Double.PositiveInfinity)
    var bestModel = None : Option[LinearRegressionModel]

    for (numIterations <- numIterationsArray) {
      for (stepSize <- stepSizeArray) {

        val model = LinearRegressionWithSGD.train(parsedTraining, numIterations, stepSize)

        val trainMSE = getMSE(parsedTraining, model)
        println("NUM ITERATIONS: " + numIterations + ";   STEP SIZE: " + stepSize)
        println("Training Data Mean Squared Error = " + trainMSE)

        val testMSE = getMSE(parsedTest, model)
        println("Test Data Mean Squared Error = " + testMSE)

        if (testMSE < currentMinMSE) {
          currentMinMSE = testMSE
          bestNumIterations = numIterations
          bestStepSize = stepSize
          bestModel = Some(model)
        }

      }
    }

    println("-------------------------------------------------------")
    println("Best Choice: NUM ITERATIONS " + bestNumIterations + ", STEP SIZE " + bestStepSize)
    println("  with MSE " + currentMinMSE)

    bestModel match {
      case None => Console.err.println("Error! No model is saved!")
      case Some(value) => value.save(sc, dir)
    }

    println("Optimization complete. Exit.")
    println("-------------------------------------------------------")

  }

  def runTestLR(sc: SparkContext, dir: String, data: RDD[Array[Double]]) {

    val model = LinearRegressionModel.load(sc, dir)
    val parsed = parseData(data)

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

  def runPredLR(sc: SparkContext, dir: String, data: RDD[Array[Double]]) {

    val model = LinearRegressionModel.load(sc, dir)
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
