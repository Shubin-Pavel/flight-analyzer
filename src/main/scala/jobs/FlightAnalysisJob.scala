package com.example
package jobs

import commit.OutputCommitter
import config.JobConfig
import metrics.Metrics
import preprocessing.DataMart._
import preprocessing.RawData._
import readers.CsvReader
import schemas.Schemas
import transformers.Transformers._
import types.SortDirection

import com.example.writers.CsvWriter
import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import java.sql.{Date, Timestamp}
import java.time.LocalDateTime

final class FlightAnalysisJob(config: JobConfig)(implicit spark: SparkSession) extends Job {
  private val log: Logger = LogManager.getLogger(getClass)
  private val reader = new CsvReader()
  private val writer = new CsvWriter()
  private val metaDir: String = "meta"
  private val dataMartDir: String = "datamart"
  private val reportsDir: String = "reports"
  private val defaultDate: Date = Date.valueOf("2014-12-31")
  private val targetPath: Path = new Path(config.outputPath, "output")
  private val tempPath: Path = new Path(config.outputPath, "temp")
  private val metaPath: Path = new Path(targetPath, metaDir)
  private val reportsPath: Path = new Path(targetPath, reportsDir)
  private val dataMartPath: Path = new Path(targetPath, dataMartDir)
  private val originAirportAirlineDataMartPath: Path = new Path(dataMartPath, "originAirportAirlineDataMart")
  private val originToDestinationDataMartPath: Path = new Path(dataMartPath, "originToDestinationDataMart")
  private val delayDataMartPath: Path = new Path(dataMartPath, "delayDataMart")
  private implicit val sortDirection: SortDirection = config.sortDirection

  override def run(): Unit = {
    log.info("Job started")

    val fs = targetPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val hasPreviousState = fs.exists(new Path(metaPath, "meta_info"))
    val hasOriginAirportAirlineDataMartOld = fs.exists(originAirportAirlineDataMartPath)
    val hasOriginToDestinationDataMartOld = fs.exists(originToDestinationDataMartPath)
    val hasDelayDataMartOld = fs.exists(delayDataMartPath)

    val airlinesDf = reader.read(config.airlinesPath, Schemas.airlines)
    val airportsDf = reader.read(config.airportsPath, Schemas.airports)
    val flightsDf = reader.read(config.flightsPath, Schemas.flights)

    val metaDf: DataFrame = getMeta(hasPreviousState)
      .persist()

    val (lastRunDt, lastSorted): (Date, String) = metaDf
      .transform(getLastRun(Seq.empty[Column], Seq(col("collected").desc, col("processed").desc)))
      .collect()
      .headOption
      .map(row => (row.getAs[Date]("collected"), row.getAs[String]("sorted")))
      .getOrElse(defaultDate, "desc")

    log.info(s"New last run period $lastRunDt")
    log.info(s"New last run sorted $lastSorted")

    val prepareAirlinesDf = airlinesDf
      .transform(prepareAirlines)
    val prepareAirportsDf = airportsDf
      .transform(prepareAirports)
    val bPrepareAirlinesDf = broadcast(prepareAirlinesDf)
    val bPrepareAirportsDf = broadcast(prepareAirportsDf)
    val prepareFlightsDf = flightsDf
      .transform(prepareFlights(bPrepareAirlinesDf, bPrepareAirportsDf))

    prepareAirlinesDf
      .unpersist()

    val newFlightsDf =
      prepareFlightsDf
        .transform(filterByCond(col("period_dt") > lit(lastRunDt)))
        .persist()

    val sortFlg: Boolean = SortDirection.fromString(lastSorted) == sortDirection

    if (newFlightsDf.isEmpty && sortFlg) {
      log.warn("No new YEAR/MONTH periods found")
      log.warn(s"Get reports in the directory ${reportsPath.toString}")
      log.info("Job completed with no changes")
    } else {
      log.info("New periods/sorted detected")
      val dataMartFlg = Map(
        "hasOriginAirportAirlineDataMartOld" -> hasOriginAirportAirlineDataMartOld,
        "hasOriginToDestinationDataMartOld" -> hasOriginToDestinationDataMartOld,
        "hasDelayDataMartOld" -> hasDelayDataMartOld,
      )
      val allDataMarts = calcAllDataMart(newFlightsDf, dataMartFlg)

      val newLastDt = allDataMarts("delayDataMartDf")
        .select(col("max_period_dt"))
        .collect()
        .headOption
        .map(row => row.getAs[Date]("max_period_dt"))
        .getOrElse(lastRunDt)
      val nowDtTm = Timestamp.valueOf(LocalDateTime.now())
      val rowMeta = Row(newLastDt, nowDtTm, sortDirection.toString)
      val newRowMetaDf = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(rowMeta)),
        Schemas.metaInfo
      )
      val newMetaDf = metaDf.unionByName(newRowMetaDf)

      val prepareAllDataMarts = allDataMarts.map { case (key, df) =>
        val newDf = if (key == "delayDataMartDf") df.drop("max_period_dt").persist() else df.persist()
        (key, newDf)
      }
      val allReports = calcAllReports(prepareAllDataMarts)
      try {
        if (fs.exists(tempPath)) {
          fs.delete(tempPath, true)
        }

        if (!fs.mkdirs(tempPath)) {
          throw new IllegalStateException(
            s"Cannot create temporary directory [$tempPath]"
          )
        }

        if (!newFlightsDf.isEmpty) dataMartsWriter(prepareAllDataMarts, new Path(tempPath, dataMartDir))
        else log.info(s"DataMart already exists ${dataMartPath.toString}")

        reportsWriter(allReports, new Path(tempPath, reportsDir))

        metaWriter(newMetaDf, new Path(tempPath, metaDir))

        new OutputCommitter(fs).commit(tempPath, targetPath)

        log.info("Job finished successfully")
      } catch {
        case e: Throwable =>
          log.error(s"Job failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}", e)
          throw e
      }
    }
  }

  private def getMeta(exists: Boolean): DataFrame  = {
    if (exists) reader.read(new Path(metaPath, "meta_info").toString, Schemas.metaInfo)
    else spark.createDataFrame(spark.sparkContext.emptyRDD[Row], Schemas.metaInfo)
  }

  private def getLastRun(groupField: Seq[Column], orderedField: Seq[Column], top: Int = 1)(df: DataFrame): DataFrame = {
    val window = Window
      .partitionBy(groupField: _*)
      .orderBy(orderedField: _*)
    df
      .withColumn("_rn", row_number().over(window))
      .filter(col("_rn") <= lit(top))
      .drop("_rn")
  }

  private def calcAllDataMart(df: DataFrame, dataMartFlg: Map[String, Boolean]): Map[String, DataFrame] = {
    val originAirportAirlineDataMartNewDf = df.transform(originAirportAirlineDataMart)
    val originToDestinationDataMartNewDf = df.transform(originToDestinationDataMart)
    val delayDataMartNewDf = df.transform(delayDataMart)

    val originAirportAirlineDataMartOldDf =
      if (dataMartFlg("hasOriginAirportAirlineDataMartOld"))
        reader.read(originAirportAirlineDataMartPath.toString, Schemas.originAirportAirline)
      else spark.createDataFrame(spark.sparkContext.emptyRDD[Row], Schemas.originAirportAirline)
    val originToDestinationDataMartOldDf =
      if (dataMartFlg("hasOriginToDestinationDataMartOld"))
        reader.read(originToDestinationDataMartPath.toString, Schemas.originToDestination)
      else spark.createDataFrame(spark.sparkContext.emptyRDD[Row], Schemas.originToDestination)
    val delayDataMartOldDf =
      if (dataMartFlg("hasDelayDataMartOld"))
        reader.read(delayDataMartPath.toString, Schemas.delay)
      else spark.createDataFrame(spark.sparkContext.emptyRDD[Row], Schemas.delay)

    val originAirportAirlineDataMartDf = originAirportAirlineDataMartNewDf
      .transform(mergeDataFrames(
        originAirportAirlineDataMartOldDf,
        Seq("origin_airport", "airline_name", "day_of_week")
      ))
    val originToDestinationDataMartDf = originToDestinationDataMartNewDf
      .transform(mergeDataFrames(
        originToDestinationDataMartOldDf,
        Seq("origin_airport", "destination_airport")
      ))
    val delayDataMartDf = delayDataMartNewDf
      .transform(mergeDataFrames(
        delayDataMartOldDf,
        Seq.empty[String]
      ))

    Map(
      "originAirportAirlineDataMartDf" -> originAirportAirlineDataMartDf,
      "originToDestinationDataMartDf" -> originToDestinationDataMartDf,
      "delayDataMartDf" -> delayDataMartDf,
    )
  }

  private def calcAllReports(dataMarts: Map[String, DataFrame]): Map[String, DataFrame] = {
    val top10AirportsDf = dataMarts("originAirportAirlineDataMartDf")
      .transform(Metrics.topAirports)
      .transform(topN(Seq(lit(1)), "total_flights", 10))

    val top10AirlineOnTimeDf = dataMarts("originAirportAirlineDataMartDf")
      .transform(Metrics.topAirlineOnTime)
      .transform(topN(Seq(lit(1)), "total_completed_on_time_flights", 10))

    val top10carrierByAirportDf = dataMarts("originAirportAirlineDataMartDf")
      .transform(Metrics.carrierByAirport)
      .transform(topN(Seq(col("origin_airport")), "total_departure_on_time_flights", 10))

    val top10destinationByAirportDf = dataMarts("originToDestinationDataMartDf")
      .transform(Metrics.destinationByAirport)
      .transform(topN(Seq(col("origin_airport")), "total_departure_on_time_flights", 10))

    val weekdayArrivalOnTimeDf = dataMarts("originAirportAirlineDataMartDf")
      .transform(Metrics.weekdayArrival)
      .transform(orderedDf("total_arrival_on_time_flights"))

    val delayReasonsDf = dataMarts("delayDataMartDf")
      .transform(Metrics.delayReasons)

    Map(
      "top10AirportsDf" -> top10AirportsDf,
      "top10AirlineOnTimeDf" -> top10AirlineOnTimeDf,
      "top10carrierByAirportDf" -> top10carrierByAirportDf,
      "top10destinationByAirportDf" -> top10destinationByAirportDf,
      "weekdayArrivalOnTimeDf" -> weekdayArrivalOnTimeDf,
      "delayReasonsDf" -> delayReasonsDf,
    )
  }

  private def reportsWriter(reports: Map[String, DataFrame], reportsPath: Path): Unit = reports.foreach {
    case (key, reportDf) => {
      writer.write(reportDf, new Path(reportsPath, key.dropRight(2)).toString)
      log.info(s"Report ${key.dropRight(2)} written to: ${new Path(reportsPath, key.dropRight(2))}")
    }
  }

  private def dataMartsWriter(dataMarts: Map[String, DataFrame], dataMartPath: Path): Unit = dataMarts.foreach {
    case (key, dataMartDf) => {
      writer.write(dataMartDf, new Path(dataMartPath, s"${key.dropRight(2)}").toString)
      log.info(s"DataMart ${key.dropRight(2)} written to: $dataMartPath")
    }
  }

  private def metaWriter(df: DataFrame, metaPath: Path): Unit = {
    writer.write(df, new Path(metaPath, "meta_info").toString)
    log.info(s"Meta information written to: ${new Path(metaPath, "meta_info")}")
  }
}
