package helper

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import helper.Helper._
import ml.KM._

object BikeHelper {

	def getStartAndEndBike(sc: SparkContext, dir: String) : (RDD[Vector], RDD[Vector], RDD[Array[String]]) = {
		val trips = sc.textFile(dir).map(rec => rec.split(","))
		val parsedStart = trips.map(items => Vectors.dense(items(4).toDouble, items(5).toDouble))
		val parsedEnd = trips.map(items => Vectors.dense(items(7).toDouble, items(8).toDouble))
		return (parsedStart, parsedEnd, trips)
	}

	def parseBikeData(sc: SparkContext, dir: String, kmeans: String) : RDD[(String, (Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double))] = {
		val (parsedStart, parsedEnd, trips) = getStartAndEndBike(sc, dir)
		val clusters = runKM(sc, parsedEnd, kmeans)
		return getModifiedDataBike(parsedStart, parsedEnd, trips, clusters)
	}

	def getModifiedDataBike(parsedStart: RDD[Vector], parsedEnd: RDD[Vector], trips: RDD[Array[String]], clusters: KMeansModel) : RDD[(String, (Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double))] = {
		val predStart = clusters.predict(parsedStart)
		val predEnd = clusters.predict(parsedEnd)
		val combined = trips.zip(predStart).map{ case (v1,v2) => v1 ++ Array(v2.toString) }.zip(predEnd).map{ case (v1,v2) => v1 ++ Array(v2.toString) }
		val bikeData = combined.map(items => (items(0), (minutes(items(1)), items(2).toDouble,
																items(3).toDouble, items(4).toDouble,
																items(5).toDouble, items(6).toDouble,
																items(7).toDouble, items(8).toDouble,
															  items(9).toDouble, items(10).toDouble,
															  items(11).toDouble, items(12).toDouble)))
		return bikeData
	}

	def parseBikeDataKM(sc: SparkContext, dir: String, kmeans: String) : RDD[(String, (Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double))] = {
	  val (parsedStart, parsedEnd, trips) = getStartAndEndBike(sc, dir)
	  val clusters = KMeansModel.load(sc, kmeans)
		return getModifiedDataBike(parsedStart, parsedEnd, trips, clusters)
	}

	def joinBikeData(weatherData: RDD[(String, (Double, Double, Double, Double))], bikeData: RDD[(String, (Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double))]) : RDD[Array[Double]] = {
	  val dataPrep = bikeData.join(weatherData)
	    .map{ case (dt, ((tm,dur,id1,lat1,lon1,id2,lat2,lon2,yr,gdr,km1,km2), (temp,rain,snow,wspd))) =>
	                Array(dur,days(dt),tm,id1,lat1,lon1,id2,lat2,lon2,km1,km2,euclidean(cartesian(lat1,lon1),cartesian(lat2,lon2)),weekday(dt),peak(dt,tm),yr,gdr,temp,rain,snow,wspd)}
	    .filter(items => items(0) >= 120.0)
	    .filter(items => items(0) <= 3600.0)
		val (avg, stdDev) = getAvgAndStdDevBike(dataPrep)
		val data = dataPrep.filter{
			items => {
				val speed = items(11) / items(0)
				(speed < avg + 3 * stdDev && speed > avg - 3 * stdDev)
			}
		}
	  return data
	}

	def getJoinedBikeData(sc: SparkContext, weatherData: RDD[(String, (Double, Double, Double, Double))], dir: String, kmeans: String) : RDD[Array[Double]] = {
		val bikeData = parseBikeData(sc, dir, kmeans)
	  val data = joinBikeData(weatherData, bikeData)
		return data
	}

	def getJoinedBikeDataKM(sc: SparkContext, weatherData: RDD[(String, (Double, Double, Double, Double))], dir: String, kmeans: String) : RDD[Array[Double]] = {
		val bikeData = parseBikeDataKM(sc, dir, kmeans)
	  val data = joinBikeData(weatherData, bikeData)
		return data
	}

	def getAvgAndStdDevBike(dataPrep: RDD[Array[Double]]) : (Double, Double) = {
		val speedRDD = dataPrep.map(items => items(11) / items(0))
		val avg = speedRDD.mean
		val devRDD = speedRDD.map(spd => Math.pow(spd - avg, 2))
		val stdDev = Math.sqrt(devRDD.sum / devRDD.count)
		return (avg, stdDev)
	}

	def getStartAndEndBikePred(sc: SparkContext, dir: String) : (RDD[Vector], RDD[Vector], RDD[Array[String]]) = {
		val trips = sc.textFile(dir).map(rec => rec.split(","))
		val parsedStart = trips.map(items => Vectors.dense(items(3).toDouble, items(4).toDouble))
		val parsedEnd = trips.map(items => Vectors.dense(items(6).toDouble, items(7).toDouble))
		return (parsedStart, parsedEnd, trips)
	}

	def getModifiedDataBikePred(parsedStart: RDD[Vector], parsedEnd: RDD[Vector], trips: RDD[Array[String]], clusters: KMeansModel) : RDD[(String, (Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double))] = {
		val predStart = clusters.predict(parsedStart)
		val predEnd = clusters.predict(parsedEnd)
		val combined = trips.zip(predStart).map{ case (v1,v2) => v1 ++ Array(v2.toString) }.zip(predEnd).map{ case (v1,v2) => v1 ++ Array(v2.toString) }
		val bikeData = combined.map(items => (items(0), (minutes(items(1)), items(2).toDouble,
																items(3).toDouble, items(4).toDouble,
																items(5).toDouble, items(6).toDouble,
																items(7).toDouble, items(8).toDouble,
															  items(9).toDouble, items(10).toDouble,
															  items(11).toDouble)))
		return bikeData
	}

	def joinBikeDataPred(weatherData: RDD[(String, (Double, Double, Double, Double))], bikeData: RDD[(String, (Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double))]) : RDD[Array[Double]] = {
	  val dataPrep = bikeData.join(weatherData)
	    .map{ case (dt, ((tm,id1,lat1,lon1,id2,lat2,lon2,yr,gdr,km1,km2), (temp,rain,snow,wspd))) =>
	                Array(days(dt),tm,id1,lat1,lon1,id2,lat2,lon2,km1,km2,euclidean(cartesian(lat1,lon1),cartesian(lat2,lon2)),weekday(dt),peak(dt,tm),yr,gdr,temp,rain,snow,wspd)}
	  return dataPrep
	}

	def parseBikeDataPred(sc: SparkContext, dir: String, kmeans: String) : RDD[(String, (Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double))] = {
		val (parsedStart, parsedEnd, trips) = getStartAndEndBikePred(sc, dir)
		val clusters = KMeansModel.load(sc, kmeans)
		return getModifiedDataBikePred(parsedStart, parsedEnd, trips, clusters)
	}

	def getJoinedBikeDataPred(sc: SparkContext, weatherData: RDD[(String, (Double, Double, Double, Double))], dir: String, kmeans: String) : RDD[Array[Double]] = {
		val bikeData = parseBikeDataPred(sc, dir, kmeans)
	  val data = joinBikeDataPred(weatherData, bikeData)
		return data
	}

}
