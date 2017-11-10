package com.eleks.nataliya.bartkiv

import org.apache.spark.sql.SparkSession

class SparkSqlHelper(spark : SparkSession) {
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

    def createTable[T](tableName : String, dataType : Class[T], delimiter : String, compression : String) : Unit = {
        val fields = new StringBuilder

        //TODO : Convert BigDecimal to decimal somehow
        for(field <- dataType.getDeclaredFields) {
            val fieldType = field.getType.getSimpleName
            val fieldName = field.getName
            fields.append(s"${fieldName} ${fieldType.toUpperCase()}, ")
        }
        fields.deleteCharAt(fields.lastIndexOf(","))


        val query = s"""CREATE TABLE IF NOT EXISTS $tableName
            (${fields.toString()})
            PARTITIONED BY (month INT)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY '${delimiter}'
            STORED AS TEXTFILE
            TBLPROPERTIES('orc.compress'='${compression}')"""

        println(query)
    }
}
