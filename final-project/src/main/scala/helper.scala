package helper

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.regression.LinearRegressionModel

object Helper {

  def days(day: String) : Double = {
    val dayFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val target = new java.text.SimpleDateFormat("D")
    val dayParse = target.format(dayFormat.parse(day)).toDouble
    val dayLast = target.format(dayFormat.parse(day.split("-")(0)+"-12-31")).toDouble
    return dayParse / dayLast
  }

  def minutes(time: String) : Double = {
    val arr = time.split(":").map(_.toDouble)
    val mins = arr(2) + arr(1) * 60 + arr(0) * 3600
    return mins / 86400
  }

  def weekday(day: String) : Double = {
    val dayFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val target = new java.text.SimpleDateFormat("E")
    val dayParse = target.format(dayFormat.parse(day))
    if (dayParse != "Sat" && dayParse != "Sun") return 1.0
    return 0.0
  }

  def peak(day: String, minute: Double) : Double = {
    if (weekday(day) == 0.0) return 0.0
    val (morningPeakStart, morningPeakEnd) = (minutes("06:00:00"), minutes("10:00:00"))
    val (afternoonPeakStart, afternoonPeakEnd) = (minutes("16:00:00"), minutes("20:00:00"))
    if ((minute >= morningPeakStart && minute <= morningPeakEnd) || (minute >= afternoonPeakStart && minute <= afternoonPeakEnd)) return 1.0
    return 0.0
  }

  def cartesian(latitude: Double, longitude: Double) : (Double, Double, Double) = {
    val lat = Math.toRadians(latitude)
    val lon = Math.toRadians(longitude)
    val x = Math.cos(lat) * Math.cos(lon)
    val y = Math.cos(lat) * Math.sin(lon)
    val z = Math.sin(lat)
    return (x,y,z)
  }

  def euclidean(loc1: (Double, Double, Double), loc2: (Double, Double, Double)) : Double = {
    val (x1,y1,z1) = loc1
    val (x2,y2,z2) = loc2
    return Math.sqrt(Math.pow(x1-x2, 2) + Math.pow(y1-y2, 2) + Math.pow(z1-z2, 2)) * 6371000
  }

  def getTrainingAndTest(data: RDD[Array[Double]]) : (RDD[LabeledPoint], RDD[LabeledPoint]) = {
    val Array(training, test) = data.randomSplit(Array(0.7,0.3))
    val parsedTraining = parseData(training)
    val parsedTest = parseData(test)
    return (parsedTraining, parsedTest)
  }

  def parseData(data: RDD[Array[Double]]) : RDD[LabeledPoint] = {
    val parsed = data.map { items =>
      val d = items.slice(1,items.size)
      val v = Vectors.dense(d)
      LabeledPoint(items(0), v)
    }
    return parsed
  }

  def parseWeatherData(sc: SparkContext, dir: String) : RDD[(String, (Double, Double, Double, Double))] = {
    val weatherData = sc.textFile(dir)
      .filter(rec => rec.startsWith("2"))
      .map(rec => rec.split(","))
      .map(items => (items(0), (items(1).toDouble, items(2).toDouble,
                                items(3).toDouble, items(4).toDouble)))
    return weatherData
  }

  def getMSE(input: RDD[LabeledPoint], model: DecisionTreeModel) : Double = {
    val valuesAndPreds = input.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    return valuesAndPreds.map{ case (v,p) => math.pow(v-p, 2) }.mean()
  }

  def getMSE(input: RDD[LabeledPoint], model: RandomForestModel) : Double = {
    val valuesAndPreds = input.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    return valuesAndPreds.map{ case (v,p) => math.pow(v-p, 2) }.mean()
  }

  def getMSE(input: RDD[LabeledPoint], model: LinearRegressionModel) : Double = {
    val valuesAndPreds = input.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    return valuesAndPreds.map{ case (v,p) => math.pow(v-p, 2) }.mean()
  }

}
