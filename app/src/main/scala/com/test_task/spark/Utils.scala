package com.test_task.spark

import org.joda.time.DateTime.now
import org.joda.time.DateTimeZone

import java.sql.Timestamp


object Utils {

  def getTimestampNow: Timestamp = new java.sql.Timestamp(now(DateTimeZone.UTC).getMillis)

  def getCurrentWeek: Int = now(DateTimeZone.UTC).getWeekOfWeekyear

  def getCurrentYear: Int = now(DateTimeZone.UTC).getYear

  def getCurrentMin: Int = now(DateTimeZone.UTC).getMinuteOfDay

  def getCurrentHour: Int = now(DateTimeZone.UTC).getHourOfDay

}
