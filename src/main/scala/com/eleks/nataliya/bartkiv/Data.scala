package com.eleks.nataliya.bartkiv

import java.sql.Timestamp

case class Data(id : Int,
                value : Double,
                datetime : Timestamp,
                latitude : BigDecimal,
                longitude : BigDecimal)
