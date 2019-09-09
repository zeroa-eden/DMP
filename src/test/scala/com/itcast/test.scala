package com.itcast

import com.typesafe.config.{Config, ConfigFactory}

object test {
  def main(args: Array[String]): Unit = {
    val config: Config = ConfigFactory.load("test")
    val name: String = config.getString("foo.name")
    val age: String = config.getString("foo.age")
    println(name )
    println(age)
  }
}
