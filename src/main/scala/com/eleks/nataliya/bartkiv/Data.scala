package com.eleks.nataliya.bartkiv

import java.sql.Timestamp

case class Data(id : Int,
                value : Double,
                timestamp : Timestamp,
                latitude : BigDecimal,
                longitude : BigDecimal)
