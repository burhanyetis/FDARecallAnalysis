package com.dataprocessing.spark

object Main {
  def main(args: Array[String]): Unit = {
    val dataProcessor = new DataProcessor()
    dataProcessor.processData()
  }
}