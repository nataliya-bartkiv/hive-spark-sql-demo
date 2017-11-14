package com.eleks.nataliya.bartkiv

import java.sql.Timestamp
import java.io._

import scala.util.Random

object Generator {
    val maxValue = 1000
    val maxId = 100
    val initData : Location = Location(
        id = Random.nextInt(maxValue),
        value = nextValue(maxValue),
        datetime = new Timestamp(System.nanoTime()),
        longitude = 23.998301,
        latitude = 49.803899
    )

    var currentData : Location = initData

    def nextData() : Location = {
        val id = Random.nextInt(maxId)
        val value = nextValue(maxValue)
        //TODO: Generate random but correct timestamp (each file - one month)
        val timestamp = new Timestamp(System.currentTimeMillis())
        val longitude = nextCoordinate(currentData.longitude)
        val latitude = nextCoordinate(currentData.latitude)

        currentData = Location(id, value, timestamp, latitude, longitude)
        currentData
    }

    private def nextCoordinate(previous : Double) : Double = {
        val offset = Math.random() / 100
        val current = previous + offset
        BigDecimal.apply(current).setScale(6, BigDecimal.RoundingMode.HALF_UP).toDouble
    }

    private def nextValue(max : Int) : Double = {
        Math.random() * max
    }

    def nextFile(path : String, recordsCount : Int, delimiter : String): Unit = {
        val file = new File(path)
        val writer = new BufferedWriter(new FileWriter(file))

        for(_ <- 0 until recordsCount) {
            val data = nextData()
            val dataString = data.productIterator.mkString(delimiter) + "\n"
            writer.write(dataString)
        }
        writer.close()
    }
}
