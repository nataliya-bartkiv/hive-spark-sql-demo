package com.eleks.nataliya.bartkiv

import java.io._
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession

class SparkSql(spark : SparkSession) {
    def createDatabase(name : String): Unit = {
        val query = s"CREATE DATABASE IF NOT EXISTS ${name} "
        spark.sql(query)
    }

    def useDatabase(name: String): Unit = {
        val query = s"USE ${name}"
        spark.sql(query)
    }

    def loadDataLocal(tableName : String, filePath : String, partition : String, partitionValue : String): Unit = {
        val query = s"LOAD DATA LOCAL INPATH '${filePath}' " +
            s"INTO TABLE ${tableName} " +
            s"PARTITION(${partition}='${partitionValue}')"

        spark.sql(query)
    }

    def loadDataLocal(tableName : String, filePath : String): Unit = {
        val file = new File(filePath)
        val bufferedReader = new BufferedReader(new FileReader(file))
        val line: String = bufferedReader.readLine()
        val fields = line.split('|')
        val timestampIndex = 2

        //TODO : Put extracting month into separate function
        val calendar = Calendar.getInstance()
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")
        val date = dateFormat.parse(fields(timestampIndex))
        calendar.setTime(date)
        val month = calendar.get(Calendar.MONTH) + 1

        //TODO : Generalize loading somehow
        val query = s"LOAD DATA LOCAL INPATH '${filePath}' " +
            s"INTO TABLE ${tableName} " +
            s"PARTITION(month='${month}')"

        spark.sql(query)
        bufferedReader.close()
    }

    def createTable[T](tableName : String, dataType : Class[T], delimiter : String, compression : String) : Unit = {
        val fields = new StringBuilder

        for(field <- dataType.getDeclaredFields) {
            val fieldType = field.getType.getSimpleName
            val fieldName = field.getName
            fields.append(s"${fieldName} ${fieldType.toUpperCase()}, ")
        }
        fields.deleteCharAt(fields.lastIndexOf(","))

        //TODO: Generalize creating partitions somehow
        val query = s"""CREATE TABLE IF NOT EXISTS $tableName
            (${fields.toString()})
            PARTITIONED BY (month INT)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY '${delimiter}'
            STORED AS TEXTFILE
            TBLPROPERTIES('orc.compress'='${compression}')"""

        spark.sql(query)
    }
}
