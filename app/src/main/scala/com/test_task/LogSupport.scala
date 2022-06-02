package com.test_task

import org.apache.log4j.Logger


trait LogSupport {
  @transient protected  lazy val log: Logger = org.apache.log4j.LogManager.getLogger(getClass)
}
