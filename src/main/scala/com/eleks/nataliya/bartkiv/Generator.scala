package com.eleks.nataliya.bartkiv

import java.sql.Timestamp

object Generator {
    val maxValue = 1000
    val initData : Data = Data(
        id = 1,
        value = nextValue(maxValue),
        timestamp = new Timestamp(System.nanoTime()),
        longitude = 23.998301,
        latitude = 49.803899
    )

    var currentData : Data = initData

    def nextData() : Data = {
        val id = currentData.id + 1
        val value = nextValue(maxValue)
        val timestamp = new Timestamp(System.nanoTime())
        val longitude = nextCoordinate(currentData.longitude)
        val latitude = nextCoordinate(currentData.latitude)

        currentData = Data(id, value, timestamp, latitude, longitude)
        currentData
    }

    private def nextCoordinate(previous : BigDecimal) : BigDecimal = {
        val offset = Math.random() / 100
        val current = previous + offset
        current.bigDecimal.setScale(6, BigDecimal.RoundingMode.HALF_UP)
    }

    private def nextValue(max : Int) : Double = {
        Math.random() * max
    }
}
