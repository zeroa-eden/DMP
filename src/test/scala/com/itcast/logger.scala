package com.itcast

import org.slf4j.{Logger, LoggerFactory}

object logger {
  @transient private val logger: Logger = LoggerFactory.getLogger(
    this.getClass.getName.stripSuffix("$")
  )

}
