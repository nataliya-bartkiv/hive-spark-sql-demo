package com.eleks.nataliya.bartkiv

object Main {
    def main(args : Array[String]) : Unit = {

        val f = new SparkSqlHelper(null)
        f.createTable("data", classOf[Data], "|", "snappy")
    }
}
