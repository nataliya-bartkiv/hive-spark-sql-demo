package com.eleks.nataliya.bartkiv
import org.apache.spark.sql.Encoders

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class GeneralSparkSql(spark : SparkSession) {
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

    def createTable[T](tableName : String, delimiter : String, compression : String, dataType : Class[T]) : Unit = {
        val fields = new StringBuilder

        for(field <- dataType.getDeclaredFields) {
            val fieldType = field.getType.getSimpleName
            val fieldName = field.getName
            fields.append(s"${fieldName} ${fieldType.toUpperCase()}, ")
        }
        fields.deleteCharAt(fields.lastIndexOf(","))

        val query = s"""CREATE TABLE IF NOT EXISTS $tableName
            (${fields.toString()})
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY '${delimiter}'
            STORED AS TEXTFILE
            TBLPROPERTIES('orc.compress'='${compression}')"""

        spark.sql(query)
    }

    def read(database : String, table : String) : DataFrame = {
        val query = s"SELECT * FROM $database.$table"
        spark.sql(query)
    }

    def readAsDataset[T](database : String, table : String, dataType : Class[T]) : Dataset[T] = {
        val dataFrame = read(database, table)
        val dataEncoder = Encoders.bean(dataType)
        dataFrame.as[T](dataEncoder)
    }

    def saveAsTable(inputDF : DataFrame, tablename : String) : Unit = {
        inputDF.write.saveAsTable(tablename)
    }
}
