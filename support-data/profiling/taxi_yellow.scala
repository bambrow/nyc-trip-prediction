import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col

//YellowTaxi
object ProcessYellowTaxi {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("Data processing yellow Taxi")
    	val sc = new SparkContext(conf)
    	sc.setLogLevel("ERROR")

    	val sqlContext = new SQLContext(sc)
        //val df = sqlContext.read.format("com.databricks.spark.csv").
        //option("header","true").option("inferSchema","true").load("/user/lg2848/2016_Green_Taxi_Trip_Data.csv")

        val spark = org.apache.spark.sql.SparkSession.builder.
        //.master("local")
        appName("Data processing yellow Taxi").
        getOrCreate

        val df = spark.read.
         format("csv").
         option("inferSchema","true").
         option("header", "true"). //reading the headers
         option("mode", "DROPMALFORMED").
         load("/user/lg2848/yellowTaxi/*.csv")


        //val spark = SparkSession.builder().getOrCreate()    
        import sqlContext.implicits._ // << add this
        val timeFmt = "yyyy-MM-dd hh:mm:ss"
        val df2 = df.withColumn("Duration", (unix_timestamp(col("tpep_dropoff_datetime"), timeFmt) - 
                                             unix_timestamp(col("tpep_pickup_datetime"), timeFmt)))

        val usefulDf = df2.select(unix_timestamp(col("tpep_pickup_datetime"), timeFmt).as("pck_timestamp"), 
        	unix_timestamp(col("tpep_dropoff_datetime"), timeFmt).as("drp_timestamp"), col("Duration"), 
        	col("pickup_latitude"),col("pickup_longitude"),col("dropoff_latitude"), 
        	col("dropoff_longitude"),col("passenger_count"),col("trip_distance"))

        val dateFmt = "yyyy-MM-dd"
        val usefulDf2 = usefulDf.withColumn("pck_date", from_unixtime(col("pck_timestamp"), dateFmt))

        val hmsFmt = "HH:mm:ss"
        val usefulDf3 = usefulDf2.withColumn("pck_hms", from_unixtime(col("pck_timestamp"), hmsFmt))

        val usefulDf4 = usefulDf3.select(col("pck_date"), col("pck_hms"), col("Duration"), 
            col("pickup_latitude"),col("pickup_longitude"),col("dropoff_latitude"),col("dropoff_longitude"),
            col("passenger_count"),col("trip_distance"))

        //val partialDf = usefulDf4.limit(20000)
        //partialDf.coalesce(1).write.format("com.databricks.spark.csv").option("header", "false").save("taxiPartialDataDec")

        val usefDf5 = usefulDf4.filter(col("Duration") > lit(120))

        val usefDf6 = usefDf5.filter(col("Duration") < lit(3600))

        //val dataKeep = usefDf6.limit(9018032)

        val dataKeep1 = usefDf6.filter(col("pickup_latitude").isNotNull).
                        filter(col("pickup_longitude").isNotNull).
                        filter(col("dropoff_latitude").isNotNull).
                        filter(col("dropoff_longitude").isNotNull)
        /*
        val dataKeep2 = dataKeep1.filter(col("Pickup_latitude") =!= 0.0).
                        filter(col("Pickup_longitude") =!= 0.0).
                        filter(col("Dropoff_latitude") =!= 0.0).
                        filter(col("Dropoff_longitude") =!= 0.0).  */

        val dataKeep2 = dataKeep1.filter(col("pickup_longitude") < -73.0).
                        filter(col("pickup_longitude") > -75.0).
                        filter(col("dropoff_longitude") < -73.0).
                        filter(col("dropoff_longitude") > -75.0).
                        filter(col("pickup_latitude") < 41.5).
                        filter(col("pickup_latitude") > 39.5).
                        filter(col("dropoff_latitude") < 41.5).
                        filter(col("dropoff_latitude") > 39.5)

        println("Total count " + usefDf6.count)
        println("Useful1 count " + dataKeep1.count)
        println("Useful2 count " + dataKeep2.count)

        //val sampleData = dataKeep2.sample(false, 0.0003).limit(40000)

        dataKeep2.write.format("com.databricks.spark.csv").
        option("header", "false").save("yellowtaxiFullDataDec12")
	}
}