package com.eleks.nataliya.bartkiv

object Main {
    def main(args : Array[String]) : Unit = {
        for(_ <- 1 until 100) {
            val data = Generator.nextData()
            println(data)
        }
    }
}
