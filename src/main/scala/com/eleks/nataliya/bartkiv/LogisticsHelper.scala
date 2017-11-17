package com.eleks.nataliya.bartkiv

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window

class LogisticsHelper(spark : SparkSession) {
    def createPathDataFrame(inputDF : DataFrame) : DataFrame = {
        import org.apache.spark.sql.functions._
        val idCol = col("id")
        val valueCol = col("value")
        val datetimeCol = inputDF.col("datetime")
        val latitudeCol = inputDF.col("latitude")
        val longitudeCol = inputDF.col("longitude")

        val yearCol: Column = year(inputDF("datetime")) as "year"
        val monthCol: Column = month(inputDF("datetime")) as "month"

        val window = Window.partitionBy(idCol, yearCol, monthCol).orderBy(datetimeCol)
        val nextLatitudeCol : Column = lead(latitudeCol, 1) over window
        val nextLongitudeCol : Column = lead(longitudeCol, 1) over window
        val subdistanceCol : Column = distanceInMeters(latitudeCol, longitudeCol, nextLatitudeCol, nextLongitudeCol)
        //val subdistanceCol : Column = sqrt(pow(latitudeCol - nextLatitudeCol, 2) + pow(longitudeCol - nextLongitudeCol, 2))

        val extendedDF = inputDF.withColumn("nextLatitude", nextLatitudeCol)
            .withColumn("nextLongitude", nextLongitudeCol)
            .withColumn("subdistance", subdistanceCol)

        val reorderedColumnNames : Array[String] = Array[String] ("id", "avgValue", "distance", "year", "month")
        val avgValueCol = avg(valueCol) as "avgValue"
        val distanceCol = sum("subdistance") as "distance"
        val aggregatedDF: DataFrame = extendedDF
            .groupBy(idCol, yearCol, monthCol)
            .agg(avgValueCol, distanceCol)
            .select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)

        aggregatedDF
    }

    def readCoordinatesFile(filepath : String, delimiter : String) : DataFrame = {
        val schema =  Encoders.product[Location].schema
        spark.read
            .format("csv")
            .option("delimiter", delimiter)
            .schema(schema)
            .load(filepath)
    }

    def createPathTable (tableName : String, delimiter : String) : Unit = {
        val query = s"""CREATE TABLE IF NOT EXISTS $tableName
            (id INT, avgValue DOUBLE, distance DOUBLE)
            PARTITIONED BY (year INT, month INT)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY '${delimiter}'
            STORED AS TEXTFILE"""

        spark.sql(query)
    }

    def saveAsTable(inputDF : DataFrame, tablename : String) : Unit = {
        inputDF.show()
        inputDF.coalesce(10).write.insertInto(tablename)
    }

    def distanceInMeters(long1 : Column, lat1 : Column, long2 : Column, lat2 : Column) : Column = {
        import org.apache.spark.sql.functions._

        val r = 6371    // Earth radius in kilometers
        val phi1 = radians(lat1)
        val phi2 = radians(lat2)

        val deltaPhi = radians(lat2 - lat1)
        val deltaLambda = radians(long2 - long1)

        val a = pow(sin(deltaPhi / 2), 2) + cos(phi1) * cos(phi2) * pow(sin(deltaLambda / 2), 2)
        val c = atan2(sqrt(a), sqrt((a - 1) * -1)) * 2
        c * r
    }
}
