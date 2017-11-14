package com.eleks.nataliya.bartkiv

import java.sql.Timestamp

case class Location(id : Int,
                    value : Double,
                    datetime : Timestamp,
                    latitude : Double,
                    longitude : Double)
