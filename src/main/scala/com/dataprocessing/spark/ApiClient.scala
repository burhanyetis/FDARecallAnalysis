package com.dataprocessing.spark

import scalaj.http.Http

class ApiClient {
  def fetchData(): String = {
    val response = Http("https://api.fda.gov/food/enforcement.json").asString.body
    response
  }
}