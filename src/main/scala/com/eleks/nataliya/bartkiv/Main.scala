package com.eleks.nataliya.bartkiv

object Main {
    def main(args : Array[String]) : Unit = {

        //Set some default values
        //TODO : Put defaults into file?
        val appName = "Spark SQL Hive App"
        val master = "local[*]"
        val hivePropsPath = "config/hive.properties"

        val databaseName = "general"
        val tableName = "path"
        val compressionType = "snappy"
        val delimiter = "|"

        //Create spark session
        val sparkSession = SparkSessionManager.getSession(appName, master, hivePropsPath)
        val sparkSqlHelper = new GeneralSparkSql(sparkSession)
        val helper = new LogisticsHelper(sparkSession)

        //Generate file with mock data
        val path = "mock_data/large.txt"
        val recordsInFile = 15000
        Generator.nextFile(path, recordsInFile, delimiter)

        //Aggregate file to Path
        val inputDF = helper.readCoordinatesFile(path, delimiter)
        val aggregatedDF = helper.createPathDataFrame(inputDF)

        //Save aggregations to Hive table
        sparkSqlHelper.createDatabase(databaseName)
        sparkSqlHelper.useDatabase(databaseName)
        helper.createPathTable(tableName, delimiter)
        helper.saveAsTable(aggregatedDF, tableName)
    }
}
