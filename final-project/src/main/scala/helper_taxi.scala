package helper

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import helper.Helper._
import ml.KM._

object TaxiHelper {

	def getStartAndEndTaxi(sc: SparkContext, dir: String) : (RDD[Vector], RDD[Vector], RDD[Array[String]]) = {
		val trips = sc.textFile(dir).filter(rec => rec.startsWith("2")).map(rec => rec.split(","))
		val parsedStart = trips.map(items => Vectors.dense(items(3).toDouble, items(4).toDouble))
		val parsedEnd = trips.map(items => Vectors.dense(items(5).toDouble, items(6).toDouble))
		return (parsedStart, parsedEnd, trips)
	}

	def parseTaxiData(sc: SparkContext, dir: String, kmeans: String) : RDD[(String, (Double, Double, Double, Double, Double, Double, Double, Double, Double))] = {
		val (parsedStart, parsedEnd, trips) = getStartAndEndTaxi(sc, dir)
		val clusters = runKM(sc, parsedEnd, kmeans)
		return getModifiedDataTaxi(parsedStart, parsedEnd, trips, clusters)
	}

	def getModifiedDataTaxi(parsedStart: RDD[Vector], parsedEnd: RDD[Vector], trips: RDD[Array[String]], clusters: KMeansModel) : RDD[(String, (Double, Double, Double, Double, Double, Double, Double, Double, Double))] = {
		val predStart = clusters.predict(parsedStart)
		val predEnd = clusters.predict(parsedEnd)
		val combined = trips.zip(predStart).map{ case (v1,v2) => v1 ++ Array(v2.toString) }.zip(predEnd).map{ case (v1,v2) => v1 ++ Array(v2.toString) }
		val taxiData = combined.map(items => (items(0), (minutes(items(1)), items(2).toDouble,
																		items(3).toDouble, items(4).toDouble,
																		items(5).toDouble, items(6).toDouble,
																		items(7).toDouble, items(8).toDouble,
																		items(9).toDouble)))
		return taxiData
	}

	def parseTaxiDataKM(sc: SparkContext, dir: String, kmeans: String) : RDD[(String, (Double, Double, Double, Double, Double, Double, Double, Double, Double))] = {
		val (parsedStart, parsedEnd, trips) = getStartAndEndTaxi(sc, dir)
		val clusters = KMeansModel.load(sc, kmeans)
		return getModifiedDataTaxi(parsedStart, parsedEnd, trips, clusters)
	}

	def joinTaxiData(weatherData: RDD[(String, (Double, Double, Double, Double))], taxiData: RDD[(String, (Double, Double, Double, Double, Double, Double, Double, Double, Double))]) : RDD[Array[Double]] = {
	  val dataPrep = taxiData.join(weatherData)
	    .map{ case (dt, ((tm,dur,lat1,lon1,lat2,lon2,psg,km1,km2), (temp,rain,snow,wspd))) =>
	                Array(dur,days(dt),tm,lat1,lon1,lat2,lon2,km1,km2,euclidean(cartesian(lat1,lon1),cartesian(lat2,lon2)),weekday(dt),peak(dt,tm),psg,temp,rain,snow,wspd)}
	    .filter(items => items(0) >= 120.0)
	    .filter(items => items(0) <= 3600.0)
	  val (avg, stdDev) = getAvgAndStdDevTaxi(dataPrep)
		val data = dataPrep.filter{
			items => {
				val speed = items(9) / items(0)
				(speed < avg + 3 * stdDev && speed > avg - 3 * stdDev)
			}
		}
	  return data
	}

	def getJoinedTaxiData(sc: SparkContext, weatherData: RDD[(String, (Double, Double, Double, Double))], dir: String, kmeans: String) : RDD[Array[Double]] = {
		val taxiData = parseTaxiData(sc, dir, kmeans)
	  val data = joinTaxiData(weatherData, taxiData)
		return data
	}

	def getJoinedTaxiDataKM(sc: SparkContext, weatherData: RDD[(String, (Double, Double, Double, Double))], dir: String, kmeans: String) : RDD[Array[Double]] = {
		val taxiData = parseTaxiDataKM(sc, dir, kmeans)
		val data = joinTaxiData(weatherData, taxiData)
		return data
	}

	def getAvgAndStdDevTaxi(dataPrep: RDD[Array[Double]]) : (Double, Double) = {
		val speedRDD = dataPrep.map(items => items(9) / items(0))
		val avg = speedRDD.mean
		val devRDD = speedRDD.map(spd => Math.pow(spd - avg, 2))
		val stdDev = Math.sqrt(devRDD.sum / devRDD.count)
		return (avg, stdDev)
	}

	def getStartAndEndTaxiPred(sc: SparkContext, dir: String) : (RDD[Vector], RDD[Vector], RDD[Array[String]]) = {
		val trips = sc.textFile(dir).filter(rec => rec.startsWith("2")).map(rec => rec.split(","))
		val parsedStart = trips.map(items => Vectors.dense(items(2).toDouble, items(3).toDouble))
		val parsedEnd = trips.map(items => Vectors.dense(items(4).toDouble, items(5).toDouble))
		return (parsedStart, parsedEnd, trips)
	}

	def getModifiedDataTaxiPred(parsedStart: RDD[Vector], parsedEnd: RDD[Vector], trips: RDD[Array[String]], clusters: KMeansModel) : RDD[(String, (Double, Double, Double, Double, Double, Double, Double, Double))] = {
		val predStart = clusters.predict(parsedStart)
		val predEnd = clusters.predict(parsedEnd)
		val combined = trips.zip(predStart).map{ case (v1,v2) => v1 ++ Array(v2.toString) }.zip(predEnd).map{ case (v1,v2) => v1 ++ Array(v2.toString) }
		val taxiData = combined.map(items => (items(0), (minutes(items(1)), items(2).toDouble,
																		items(3).toDouble, items(4).toDouble,
																		items(5).toDouble, items(6).toDouble,
																		items(7).toDouble, items(8).toDouble)))
		return taxiData
	}

	def joinTaxiDataPred(weatherData: RDD[(String, (Double, Double, Double, Double))], taxiData: RDD[(String, (Double, Double, Double, Double, Double, Double, Double, Double))]) : RDD[Array[Double]] = {
	  val dataPrep = taxiData.join(weatherData)
	    .map{ case (dt, ((tm,lat1,lon1,lat2,lon2,psg,km1,km2), (temp,rain,snow,wspd))) =>
	                Array(days(dt),tm,lat1,lon1,lat2,lon2,km1,km2,euclidean(cartesian(lat1,lon1),cartesian(lat2,lon2)),weekday(dt),peak(dt,tm),psg,temp,rain,snow,wspd)}
	  return dataPrep
	}

	def parseTaxiDataPred(sc: SparkContext, dir: String, kmeans: String) : RDD[(String, (Double, Double, Double, Double, Double, Double, Double, Double))] = {
		val (parsedStart, parsedEnd, trips) = getStartAndEndTaxiPred(sc, dir)
		val clusters = KMeansModel.load(sc, kmeans)
		return getModifiedDataTaxiPred(parsedStart, parsedEnd, trips, clusters)
	}

	def getJoinedTaxiDataPred(sc: SparkContext, weatherData: RDD[(String, (Double, Double, Double, Double))], dir: String, kmeans: String) : RDD[Array[Double]] = {
		val taxiData = parseTaxiDataPred(sc, dir, kmeans)
	  val data = joinTaxiDataPred(weatherData, taxiData)
		return data
	}

}
