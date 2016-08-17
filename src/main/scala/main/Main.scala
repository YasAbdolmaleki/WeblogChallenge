package main

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

case class Log(ip: String, timeMin: String, timeMax: String, backend:String )

object Main extends App{

  val conf = new SparkConf()
    .setMaster("local[*]")  //take all available cores
    .setAppName("WeblogChallenge")
  val sc = new SparkContext(conf)
  val logFile = sc.textFile("/home/sparkdev/challenge/WeblogChallenge/data/2015_07_22_mktplace_shop_web_log_sample.log") //FIXME: HARDCODED PATH  - RDD[ String ]

  val sqlContext = new HiveContext(sc)

  import sqlContext.implicits._

  val rdd = logFile.map( line => line.split("\\s(?=([^\"]*\"[^\"]*\")*[^\"]*$)",-1))
      .map(eventRecord => {
      val datetime = DateTimeFormat.forPattern("yyyy-MM-dd\'T\'HH:mm:ss.SSSSSSZ").parseDateTime(eventRecord(0))
      val time  = DateTime.parse(eventRecord(0), DateTimeFormat.forPattern("yyyy-MM-dd\'T\'HH:mm:ss.SSSSSSZ")).toString("hh:mm:ss")
      val ipAddress = eventRecord(2)
      val serverAddress = eventRecord(3)
      Log(ipAddress, time, time, serverAddress )
    })

  var logTable = rdd.toDF()
  logTable.registerTempTable("logTable")

  val deltTimeTable = sqlContext.sql("""SELECT  T1.ip,
                                          T1.timeMin,
                                          T1.backend,
                                          COALESCE((MIN(unix_timestamp(T2.timeMin, "hh:mm:ss")) -
                                            unix_timestamp(T1.timeMin,"hh:mm:ss"))/60,0) AS TimeDiff
                                 FROM    logTable T1
                                         LEFT JOIN logTable T2
                                             ON T1.ip = T2.ip AND (unix_timestamp(T2.timeMin, "hh:mm:ss") > unix_timestamp(T1.timeMin, "hh:mm:ss"))
                                 GROUP BY T1.ip, T1.timeMin, T1.backend
                                 ORDER BY T1.ip asc""")
  deltTimeTable.registerTempTable("deltaTime")

  val aggIP = sqlContext.sql("""SELECT ip,  backend from deltaTime where TimeDiff <= 15 Group by ip, backend""")
  aggIP.show()

}
