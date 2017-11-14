package com.eleks.nataliya.bartkiv

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window


case class Aggregator(distance: Double, sumValue: Double)
case class RunningValue(distance: Double, sumValue: Double, timestamp: Timestamp, latitude : Double, longitude : Double)

class DataAggregator(spark : SparkSession) {
    def getPathLength() : Unit = {
        val filepath = "mock_data/0.txt"

        val schema = Encoders.product[Data].schema
        val df = spark.read
            .format("csv")
            .option("delimiter", "|")
            .schema(schema)
            .load(filepath)

        import org.apache.spark.sql.functions._
        val idCol = col("id")
        val valueCol = col("value")
        val datetimeCol = df.col("datetime")
        val latitudeCol = df.col("latitude")
        val longitudeCol = df.col("longitude")

        val yearCol: Column = year(df("datetime")) as "year"
        val monthCol: Column = month(df("datetime")) as "month"

        val window = Window.partitionBy(idCol, yearCol, monthCol).orderBy(datetimeCol)
        val nextLatitudeCol : Column = lead(latitudeCol, 1) over window as "nextLatitude"
        val nextLongitudeCol : Column = lead(longitudeCol, 1) over window as "nextLongitude"

        val subdistanceCol : Column = sqrt(pow(latitudeCol - nextLatitudeCol, 2) + pow(longitudeCol - nextLongitudeCol, 2))

        val extendedDF = df.withColumn("nextLatitude", nextLatitudeCol)
            .withColumn("nextLongitude", nextLongitudeCol)
            .withColumn("subdistance", subdistanceCol)


        val newDf: DataFrame = extendedDF.groupBy(idCol, yearCol, monthCol).agg(avg(valueCol) as "avgValue" , sum("subdistance") as "distance")

        newDf.show()
    }

//    def getPathLength() : Unit = {
//        val filepath = "mock_data/0.txt"
//        val df: DataFrame = spark.read.textFile(filepath).toDF()
//        import spark.implicits._
//
//        //TODO : Convert to DF with java bean object ?
//        val mapped = df.map(row => {
//            val tokens = row.getString(0).split('|')
//            val id = tokens(0).toInt
//            val value = tokens(1).toInt
//
//            val calendar = Calendar.getInstance()
//            val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")
//            val date = dateFormat.parse(tokens(2))
//            calendar.setTime(date)
//            val timestamp = new Timestamp(calendar.getTimeInMillis)
//
//            val latitude = tokens(3).toDouble
//            val longitude = tokens(4).toDouble
//
//            Data(id, value, timestamp, latitude, longitude)
//        }).toDF()
//        mapped.show()
//
//        import org.apache.spark.sql.expressions.Window
//
//        //val window = Window.partitionBy("id").orderBy("datetime")
//        //mapped.withColumn("next", avg()
//
//        import org.apache.spark.sql.functions._
//        val window = Window.partitionBy("id").orderBy("datetime")
//        val windowed = mapped.withColumn("nextLat", lead(col("latitude"), 1) over window)
//                .withColumn("nextLong", lead("longitude", 1) over window).
//
//        //windowed.
//
//    }


//    def getPathLength(): Unit = {
//        val filepath = "mock_data/0.txt"
//        val rdd = spark.sparkContext.textFile(filepath)
//        val longitudeAcc = spark.sparkContext.doubleAccumulator
//        val latitudeAcc = spark.sparkContext.doubleAccumulator
//        val countAcc = spark.sparkContext.longAccumulator
//
//        val mapped: RDD[(Int, Data)] = rdd.map[(Int, Data)](row => {
//            val tokens = row.split('|')
//            val id = tokens(0).toInt
//            val value = tokens(1).toDouble
//
//            val calendar = Calendar.getInstance()
//            val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")
//            val date = dateFormat.parse(tokens(2))
//            calendar.setTime(date)
//            val timestamp = new Timestamp(calendar.getTimeInMillis)
//
//            val latitude = tokens(3).toDouble
//            val longitude = tokens(4).toDouble
//
//            (id, Data(id, value, timestamp, latitude, longitude))
//        }).sortBy(_._2.datetime.getTime)
//
//
//        mapped.aggregateByKey(Aggregator(0, 0))(
//            (agg: Aggregator, data: Data) => {
//                if (countAcc.isZero) {
//                    countAcc.add(1)
//                    longitudeAcc.add(data.longitude)
//                    latitudeAcc.add(data.latitude)
//                    Aggregator(0, data.value)
//                } else {
//                    val distance = Math.sqrt(
//                        Math.pow(longitudeAcc.value - data.longitude, 2)
//                            + Math.pow(latitudeAcc.value - data.latitude, 2)
//                    )
//                    val sumValue = agg.sumValue + data.value
//                    countAcc.add(1)
//                    latitudeAcc.add(data.latitude - latitudeAcc.value)
//                    longitudeAcc.add(data.longitude - longitudeAcc.value)
//
//                    Aggregator(distance, sumValue)
//                }
//            },
//            (agg1: Aggregator, agg2: Aggregator) => {
//                val distance = agg1.distance + agg2.distance
//                val sumValue = agg1.sumValue + agg2.sumValue
//                Aggregator(distance, sumValue)
//            }
//        ).collect().foreach(println)
//
//        //mapped.fold()
//    }
}
