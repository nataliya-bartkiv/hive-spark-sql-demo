package com.eleks.nataliya.bartkiv

object Main {
    def main(args : Array[String]) : Unit = {

        //Set some default values
        //TODO : Put defaults into file?
        val appName = "Spark SQL Hive App"
        val master = "local[*]"
        val hivePropsPath = "config/hive.properties"

        val databaseName = "general"
        val tableName = "data"
        val compressionType = "snappy"
        val delimiter = "|"
        val recordsInFile = 1000
        val filesCount = 5

        //Generate a few files with mock data
        for(i <- 0 until filesCount) {
            val path = s"mock_data/${i}.txt"
            Generator.nextFile(path, recordsInFile, delimiter)
        }

        //Create spark session
        val sparkSession = SparkSessionManager.getSession(appName, master, hivePropsPath)
        val sparkSql = new SparkSql(sparkSession)
        //Creating database and table in it
        sparkSql.createDatabase(databaseName)
        sparkSql.useDatabase(databaseName)
        sparkSql.createTable(tableName, classOf[Data], delimiter, compressionType)

        //Loading generated data into created table
        for(i <- 0 until filesCount) {
            val path = s"mock_data/${i}.txt"
            sparkSql.loadDataLocal(tableName, path)
        }
    }
}
