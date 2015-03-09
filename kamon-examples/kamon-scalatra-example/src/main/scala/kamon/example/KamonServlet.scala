package kamon.example

import org.scalatra.{Ok, ScalatraServlet}

class KamonServlet extends ScalatraServlet {

  get("/") {
    println("blabla")
  }

  get("/timer") {
    println("timer")

  }

  get("/counter") {
    println("counter")
    "counter"
  }

  get("/histogram") {
    println("histogram")
  }
}
