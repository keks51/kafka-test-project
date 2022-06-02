package com.test_task.spark

import java.sql.Connection
import java.util.Properties
import scala.util.Try


object PostgresConn {

  case class PostgresConfig(host: String,
                            port: Int,
                            user: String,
                            pass: String,
                            db: String,
                            postgresOffsetTable: String)


  def withConn(config: PostgresConfig)(func: Connection => Unit): Try[Unit] = Try {
    val PostgresConfig(host, port, user, pass, db, _) = config
    import java.sql.DriverManager
    val url = s"jdbc:postgresql://$host:$port/$db"
    val props = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", pass)
    val conn = DriverManager.getConnection(url, props)
    func(conn)
    conn.close()
  }

}
