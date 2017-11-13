package com.eleks.nataliya.bartkiv

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionManager {
    def getConfig(appName : String, master : String, hiveSitePath : String): SparkConf = {
        val sparkConf = new SparkConf()
            .setAppName(appName)
            .setMaster(master)

        val properties = new Properties()
        val in = new FileInputStream(hiveSitePath)
        properties.load(in)

        val enumerator = properties.propertyNames()
        while (enumerator.hasMoreElements) {
            val key = enumerator.nextElement().toString
            val value = properties.getProperty(key)
            sparkConf.set(key, value)

        }
        sparkConf
    }

    def getSession(config : SparkConf) : SparkSession = {
        val sparkConf: SparkConf = config
        val spark: SparkSession = SparkSession
            .builder()
            .enableHiveSupport()
            .config(sparkConf)
            .getOrCreate()

        spark
    }

    def getSession(appName : String, master : String, hiveSitePath : String) : SparkSession = {
        val config = getConfig(appName, master, hiveSitePath)
        getSession(config)
    }
}
