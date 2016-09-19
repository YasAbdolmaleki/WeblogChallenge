package main

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

case class Log(ip: String, time: String, url: String)

object Main extends App{

  val fixed_time_window = 15
  val conf = new SparkConf()
    .setMaster("local[*]")  //take all available cores
    .setAppName("WeblogChallenge")
  val sc = new SparkContext(conf)
  val logFile = sc.textFile("/home/sparkdev/challenge/WeblogChallenge/data/2015_07_22_mktplace_shop_web_log_sample.log") //FIXME: HARDCODED PATH  - RDD[ String ]
  val sqlContext = new HiveContext(sc)

  import sqlContext.implicits._

  // parse log file - split regex
  val rdd = logFile.map( line => line.split("\\s(?=([^\"]*\"[^\"]*\")*[^\"]*$)"))
    .map(eventRecord => {
      val time  = DateTime.parse(eventRecord(0), DateTimeFormat.forPattern("yyyy-MM-dd\'T\'HH:mm:ss.SSSSSSZ")).toString("hh:mm:ss")
      val ipAddress = eventRecord(2)
      val url = eventRecord(11)
      Log(ipAddress, time, url)
    })

  var logTable = rdd.toDF()
  logTable.registerTempTable("logTable")


  /**
    * 1) Sessionize the web log by IP
    *
  **/
  val sessionize = sqlContext.sql("""select l.ip, l.time,
                                    cast(sum(l.deltaT) over (partition BY l.ip ORDER BY l.time) as varchar) as session, l.url
                                    from (select ip, time, url,
                                      case when unix_timestamp(time, "hh:mm:ss") - lag(unix_timestamp(time, "hh:mm:ss"))
                                        over (partition BY ip ORDER BY time ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) > """ + fixed_time_window + """ * 60
                                      then 1
                                      else 0
                                      end as deltaT
                                      from logTable) l""")

  sessionize.registerTempTable("sessionizedTable")
  sessionize.show()


  /**
    * 2) Determine the average session time
    *
  **/
  val sessionAverage = sqlContext.sql("""select (sum(sa.duration)/count(*)) as average_session_time
                                          from (select ip, session,
                                            (max(unix_timestamp(time, "hh:mm:ss"))-min(unix_timestamp(time, "hh:mm:ss"))) as duration
                                            from sessionizedTable
                                            group by ip, session) sa""")

  sessionAverage.registerTempTable("sessAverageTable")
  sessionAverage.show()


  /**
    * 3) Determine unique URL visits per session
    *
  **/
  val uniqueURL = sqlContext.sql("""select s.ip, s.session, count(s.url) as uniqueURL
                                      from (select ip, session, url from sessionizedTable group by ip, session, url) s
                                      group by s.ip, s.session""")

  uniqueURL.registerTempTable("uniqueURLTable")
  uniqueURL.show()


  /**
    * 4) Find the most engaged users
    *
  **/
  val longestSession = sqlContext.sql("""select uu.ip, max(uu.duration) as longestSession
                                            from(Select ip, session, (max(unix_timestamp(time, "hh:mm:ss"))-min(unix_timestamp(time, "hh:mm:ss"))) as duration
                                              from sessionizedTable
                                              group by ip, session) uu
                                            group by uu.ip
                                            order by longestSession desc
                                            limit 3""")

  longestSession.registerTempTable("longestSessionTable")
  longestSession.show()
}
