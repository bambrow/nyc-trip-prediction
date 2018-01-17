import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col

//GreenTaxi
object ProcessGreenTaxi {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("Data processing green Taxi")
    	val sc = new SparkContext(conf)
    	sc.setLogLevel("ERROR")

    	val sqlContext = new SQLContext(sc)
        val df = sqlContext.read.format("com.databricks.spark.csv").
        option("header","true").option("inferSchema","true").
        load("/user/lg2848/2016_Green_Taxi_Trip_Data.csv")

        //val spark = SparkSession.builder().getOrCreate()    
        import sqlContext.implicits._
        val timeFmt = "MM/dd/yyyy hh:mm:ss aa"
        val df2 = df.withColumn("Duration", (unix_timestamp(col("Lpep_dropoff_datetime"), timeFmt) - unix_timestamp(col("lpep_pickup_datetime"), timeFmt)))

        val usefulDf = df2.select(unix_timestamp(col("lpep_pickup_datetime"), timeFmt).as("pck_timestamp"), 
        	unix_timestamp(col("Lpep_dropoff_datetime"), timeFmt).as("drp_timestamp"), col("Duration"), 
        	col("Pickup_latitude"),col("Pickup_longitude"),col("Dropoff_latitude"), 
        	col("Dropoff_longitude"),col("Passenger_count"),col("Trip_distance"))

        val dateFmt = "yyyy-MM-dd"
        val usefulDf2 = usefulDf.withColumn("pck_date", from_unixtime(col("pck_timestamp"), dateFmt))

        val hmsFmt = "HH:mm:ss"
        val usefulDf3 = usefulDf2.withColumn("pck_hms", from_unixtime(col("pck_timestamp"), hmsFmt))

        val usefulDf4 = usefulDf3.select(col("pck_date"), col("pck_hms"), col("Duration"), col("Pickup_latitude"),col("Pickup_longitude"),col("Dropoff_latitude"),col("Dropoff_longitude"),col("Passenger_count"),col("Trip_distance"))

        //val partialDf = usefulDf4.limit(20000)
        //partialDf.coalesce(1).write.format("com.databricks.spark.csv").option("header", "false").save("taxiPartialDataDec")

        val usefDf5 = usefulDf4.filter(col("Duration") > lit(120))

        val usefDf6 = usefDf5.filter(col("Duration") < lit(3600))

        //val dataKeep = usefDf6.limit(9018032)

        val dataKeep1 = usefDf6.filter(col("Pickup_latitude").isNotNull).
                        filter(col("Pickup_longitude").isNotNull).
                        filter(col("Dropoff_latitude").isNotNull).
                        filter(col("Dropoff_longitude").isNotNull)
        /*
        val dataKeep2 = dataKeep1.filter(col("Pickup_latitude") =!= 0.0).
                        filter(col("Pickup_longitude") =!= 0.0).
                        filter(col("Dropoff_latitude") =!= 0.0).
                        filter(col("Dropoff_longitude") =!= 0.0).  */

        val dataKeep2 = dataKeep1.filter(col("Pickup_longitude") < -72.0).
                        filter(col("Pickup_longitude") > -75.0).
                        filter(col("Dropoff_longitude") < -72.0).
                        filter(col("Dropoff_longitude") > -75.0).
                        filter(col("Pickup_latitude") < 42.0).
                        filter(col("Pickup_latitude") > 39.5).
                        filter(col("Dropoff_latitude") < 42.0).
                        filter(col("Dropoff_latitude") > 39.5)

        println("Total count " + usefDf6.count)
        println("Useful1 count " + dataKeep1.count)
        println("Useful2 count " + dataKeep2.count)

        //val sampleData = dataKeep2.orderBy(rand()).limit(20000)

        dataKeep2.write.format("com.databricks.spark.csv").option("header", "false").
                  save("GreenTaxiFullDataDec11")
	}
}