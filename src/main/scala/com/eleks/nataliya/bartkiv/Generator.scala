package com.eleks.nataliya.bartkiv

import java.sql.Timestamp
import java.io._

object Generator {
    val maxValue = 1000
    val initData : Data = Data(
        id = 1,
        value = nextValue(maxValue),
        datetime = new Timestamp(System.nanoTime()),
        longitude = 23.998301,
        latitude = 49.803899
    )

    var currentData : Data = initData

    def nextData() : Data = {
        val id = currentData.id + 1
        val value = nextValue(maxValue)
        //TODO: Generate random but correct timestamp (each file - one month)
        val timestamp = new Timestamp(System.currentTimeMillis())
        val longitude = nextCoordinate(currentData.longitude)
        val latitude = nextCoordinate(currentData.latitude)

        currentData = Data(id, value, timestamp, latitude, longitude)
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
