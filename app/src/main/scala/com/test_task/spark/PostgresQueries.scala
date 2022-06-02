package com.test_task.spark

import com.test_task.LogSupport

import java.sql.{Connection, ResultSet, Statement, Timestamp}
import scala.util.Try


case class PostgresQueries(conn: Connection, offsetTable: String) extends LogSupport {

  implicit class RichResultSet(rs: ResultSet) {
    def iterate[T](f: ResultSet => T): Iterator[T] = Iterator.continually {
      if (rs.next()) Some(f(rs)) else None
    }
      .takeWhile(_.isDefined)
      .flatten
  }


  def createOffsetTable(): Unit = {
    val sql =
      s"""
        |CREATE TABLE $offsetTable (
        |	load_id serial,
        |	app_name VARCHAR ( 50 ) NOT NULL,
        |	topic VARCHAR ( 50 ) NOT NULL,
        |	start_time TIMESTAMP NOT NULL,
        | end_time TIMESTAMP,
        | start_offset bigint NOT NULL,
        | end_offset bigint NOT NULL
        |);
        |""".stripMargin
    withStmt(conn)(_.executeUpdate(sql))
  }

  def listTables: Array[String] = {
    val sql = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';"
    withStmt(conn) { stmt =>
      stmt.execute(sql)
      val rs = stmt.getResultSet
      rs.iterate(e => e.getString(1)).toArray
    }
  }

  def updateOnStart(appName: String,
                    topic: String,
                    startTime: Timestamp,
                    startOffset: Long,
                    endOffset: Long): Long = {
    val sql =
      s"""INSERT INTO $offsetTable(app_name,topic,start_time,end_time,start_offset,end_offset)
         |VALUES('$appName','$topic','$startTime',NULL,$startOffset,$endOffset);""".stripMargin
    log.info(s"Adding record into table: '$offsetTable': \n$sql")
    withStmt(conn) { stmt =>
      stmt.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS)
      val rs = stmt.getGeneratedKeys
      val rowId = rs.iterate(e => e.getLong(1)).toArray.head
      log.info(s"Successfully added record with ID: $rowId")
      rowId
    }
  }

  def getLastOffset(appName: String, topic: String): Try[Long] = Try {
    log.info(s"Trying to get last offset for APP_NAME: '$appName', TOPIC: '$topic' from $offsetTable")
    val sql =
      s"""
         |SELECT end_offset
         |FROM $offsetTable
         |WHERE app_name = '$appName' AND topic = '$topic' AND end_time IS NOT NULL
         |ORDER BY load_id
         |DESC LIMIT 1;
         |""".stripMargin
    withStmt(conn) { stmt =>
      stmt.execute(sql)
      val rs = stmt.getResultSet
      val offset = rs.iterate(e => e.getLong(1)).toArray.headOption.getOrElse(0L)
      log.info(s"Last offset for APP_NAME: '$appName', TOPIC: '$topic' from $offsetTable table is '$offset'")
      offset
    }
  }

  def updateOnEnd(rowId: Long, endTime: Timestamp): Try[Unit] = Try {
    val sql =
      s"""UPDATE $offsetTable
         |SET end_time = '$endTime'
         |WHERE load_id = $rowId""".stripMargin
    log.info(s"Updating record '$rowId' with endTime: '$endTime'")
    withStmt(conn)(_.executeUpdate(sql))
    log.info(s"Successfully updated record '$rowId' with endTime: '$endTime'")
  }

  def dropOffsetTable(): Try[Unit] = Try {
    val sql = s"""DROP TABLE $offsetTable;"""
    withStmt(conn)(_.executeUpdate(sql))
  }

  def withStmt[T](conn: Connection)(f: Statement => T): T = {
    val stmt = conn.createStatement()
    val res = f(stmt)
    stmt.close()
    res
  }

}
