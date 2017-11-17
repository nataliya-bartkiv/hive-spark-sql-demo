package com.eleks.nataliya.bartkiv

object Main {
    def main(args : Array[String]) : Unit = {

        //Set some default values
        //TODO : Put defaults into file?
        val appName = "Spark SQL Hive App"
        val master = "local[*]"
        val hivePropsPath = "config/hive.properties"
        //val hdfsPath = "hdfs://localhost:9001/user/coordinates_data"
        val hdfsPath = "hdfs://localhost:9001/user/avro_coordinates/"
        val databaseName = "general"
        val tableName = "path"
        val compressionType = "snappy"
        val delimiter = "|"

        //Create spark session
        val sparkSession = SparkSessionManager.getSession(appName, master, hivePropsPath)
        val sparkSqlHelper = new GeneralSparkSql(sparkSession)
        val helper = new LogisticsHelper(sparkSession)



        //Aggregate file to Path
        val inputDF = helper.readCoordinatesFile(hdfsPath, delimiter)
        val aggregatedDF = helper.createPathDataFrame(inputDF)

        //Save aggregations to Hive table
        sparkSqlHelper.createDatabase(databaseName)
        sparkSqlHelper.useDatabase(databaseName)
        helper.createPathTable(tableName, delimiter)
        helper.saveAsTable(aggregatedDF, tableName)
    }
}
