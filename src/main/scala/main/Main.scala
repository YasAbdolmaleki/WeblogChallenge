package main

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

case class Log(ip: String, time: String)

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
      Log(ipAddress, time )
    })

  var logTable = rdd.toDF()
  logTable.registerTempTable("logTable")


  val sessionize = sqlContext.sql("""
  select a.ip, a.time,
  cast(sum(a.new_event_boundary) OVER (PARTITION BY a.ip ORDER BY a.time) as varchar) as session from
  (select ip, time,
  case when UNIX_TIMESTAMP(time, "hh:mm:ss") - lag(UNIX_TIMESTAMP(time, "hh:mm:ss")) OVER (PARTITION BY ip ORDER BY time ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) > 15 * 60
  then 1
  ELSE 0
  END as new_event_boundary
  from logTable) a""")

  sessionize.registerTempTable("sessionizedTable")
  //sessionize.show()

  val sessionAverage = sqlContext.sql("""
    select (sum(a.duration)/count(*)) as average_session_time
    from (Select ip, session,
    (Max(UNIX_TIMESTAMP(time, "hh:mm:ss"))-Min(UNIX_TIMESTAMP(time, "hh:mm:ss"))) as duration
    from sessionizedTable
    group by ip, session) a """)

  sessionAverage.registerTempTable("sessAverageTable")
  sessionAverage.show()

}
