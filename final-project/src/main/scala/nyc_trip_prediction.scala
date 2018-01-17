import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import helper.Helper._
import ml.DT._
import ml.RF._
import ml.LR._


object NYC_Trip_Prediction {

  def main(args: Array[String]) {

    if (!validate(args)) {
      System.err.println("Usage: NYC_Trip_Prediction <weather_data_directory> <trip_data_directory> <model_directory> <kmeans_directory> <Taxi|Bike> <DT|RF|LR> <Training|Test|Prediction>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("NYC Trip Prediction")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val weatherData = parseWeatherData(sc, args(0)).cache()

    if (args(5).toLowerCase == "dt") {
      runDT(sc, args, weatherData)
    } else if (args(5).toLowerCase == "rf") {
      runRF(sc, args, weatherData)
    } else {
      runLR(sc, args, weatherData)
    }

    sc.stop()

  }

  def validate(args: Array[String]) : Boolean = {
    if (args.length < 7) return false
    val args3 = args(4).toLowerCase
    if (args3 != "taxi" && args3 != "bike") return false
    val args4 = args(5).toLowerCase
    if (args4 != "dt" && args4 != "rf" && args4 != "lr") return false
    val args5 = args(6).toLowerCase
    if (args5 != "training" && args5 != "test" && args5 != "prediction") return false
    return true
  }

}
