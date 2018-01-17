

val test2015 = sc.textFile("proj/bike/2015") // data of 2015
val test2016 = sc.textFile("proj/bike/2016") // data of 2016

// Using SimpleDateFormat to handle the dates and times.

val f1 = new java.text.SimpleDateFormat("MM/dd/yyyy")
val f2 = new java.text.SimpleDateFormat("yyyy-MM-dd")
val t = new java.text.SimpleDateFormat("HH:mm:ss")


// Data cleaning process.
// The cleaning is painful because there are lots of different formats, depending on the month of the data collected.
// Some data records are surrounded with double quotes; some have date format of yyyy-MM-dd instead of MM/dd/yyyy;
// Some only have HH:mm in the times and others have HH:mm:ss;
// In the final process, regular expression filters are used to make sure that data is clean.

val data2015a = test2015.filter(rec => (!rec.startsWith("trip") && !rec.startsWith("\"trip"))).map(rec => rec.split(','))
val data2015b = data2015a.filter(items => (items.length == 15 && items(1).contains(" ") && !items.contains("")))
val data2015c = data2015b.map(items => items.map(w => if (w.startsWith("\"")) w.substring(1,w.length()) else w)).map(items => items.map(w => if (w.endsWith("\"")) w.substring(0,w.length()-1) else w))
val data2015d = data2015c.map(items => Array(f2.format(f1.parse(items(1).split(' ')(0))), t.format(t.parse(if (items(1).split(' ')(1).split(':').length == 2) items(1).split(' ')(1)+":00" else items(1).split(' ')(1))), items(0), items(3), items(5), items(6), items(7), items(9), items(10), items(13), items(14)))
val data2015e = data2015d.filter(items => (items(2).matches("\\d+") && items(3).matches("\\d+") && items(4).matches("\\d{2}.\\d+") && items(5).matches("-\\d{2}.\\d+") && items(6).matches("\\d+") && items(7).matches("\\d{2}.\\d+") && items(8).matches("-\\d{2}.\\d+") && items(9).matches("\\d+") && items(10).matches("\\d+")))
val trip2015 = data2015e.filter(items => (items(2).toInt <= 3600 && items(2).toInt >= 120 && items(4).toDouble >= 40.4774 && items(4).toDouble <= 40.9176 && items(5).toDouble >= -74.2589 && items(5).toDouble <= -73.7004 && items(7).toDouble >= 40.4774 && items(7).toDouble <= 40.9176 && items(8).toDouble >= -74.2589 && items(8).toDouble <= -73.7004 && items(9).toInt >= 1900 && items(9).toInt <= 2010))
trip2015.cache()

val data2016a = test2016.filter(rec => (!rec.startsWith("Trip") && !rec.startsWith("\"trip"))).map(rec => rec.split(','))
val data2016b = data2016a.filter(items => (items.length == 15 && items(1).contains(" ") && !items.contains("")))
val data2016c = data2016b.map(items => items.map(w => if (w.startsWith("\"")) w.substring(1,w.length()) else w)).map(items => items.map(w => if (w.endsWith("\"")) w.substring(0,w.length()-1) else w))
val data2016d = data2016c.map(items => Array(if (items(1).split(' ')(0).contains("-")) items(1).split(' ')(0) else f2.format(f1.parse(items(1).split(' ')(0))), t.format(t.parse(if (items(1).split(' ')(1).split(':').length == 2) items(1).split(' ')(1)+":00" else items(1).split(' ')(1))), items(0), items(3), items(5), items(6), items(7), items(9), items(10), items(13), items(14)))
val data2016e = data2016d.filter(items => (items(2).matches("\\d+") && items(3).matches("\\d+") && items(4).matches("\\d{2}.\\d+") && items(5).matches("-\\d{2}.\\d+") && items(6).matches("\\d+") && items(7).matches("\\d{2}.\\d+") && items(8).matches("-\\d{2}.\\d+") && items(9).matches("\\d+") && items(10).matches("\\d+")))
val trip2016 = data2016e.filter(items => (items(2).toInt <= 3600 && items(2).toInt >= 120 && items(4).toDouble >= 40.4774 && items(4).toDouble <= 40.9176 && items(5).toDouble >= -74.2589 && items(5).toDouble <= -73.7004 && items(7).toDouble >= 40.4774 && items(7).toDouble <= 40.9176 && items(8).toDouble >= -74.2589 && items(8).toDouble <= -73.7004 && items(9).toInt >= 1900 && items(9).toInt <= 2010))
trip2016.cache()


// Construct the schema; this is just for better clarification of the choices of features.
val schema = sc.parallelize(Array(Array("date","time","duration","start-id","start-lat","start-lon","end-id","end-lat","end-lon","birthyear","gender"))).map(items => items.mkString(","))


// Combine the two RDDs using union.
// Create the data RDD for outputs.
val combined = trip2015.union(trip2016).cache()
val data = combined.map(items => items.mkString(","))
val data_with_header = schema.union(data)


trip2015.count
trip2016.count
data.count
data_with_header.take(5).foreach(println)


data.saveAsTextFile("proj/bike_prof")



