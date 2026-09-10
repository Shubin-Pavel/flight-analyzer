package com.example

import config.JobConfig
import jobs.{FlightAnalysisJob, Job}
import session.SessionWrapper

import org.apache.logging.log4j.{LogManager, Logger}

object FlightAnalyzer extends App with SessionWrapper {
  private val log: Logger = LogManager.getLogger(getClass)

  try {
    log.info("=== Flight Analyzer Job Started ===")
    log.info(s"Arguments: ${args.mkString(", ")}")

    val config: JobConfig = JobConfig.fromArgs(args)
    log.info(s"Configuration: $config")

    val job: Job = new FlightAnalysisJob(config)
    job.run()
    log.info("=== Flight Analyzer Job Completed Successfully ===")
  } catch {
    case e: IllegalArgumentException => {
      log.error("Invalid arguments provided", e)
      throw e
    }
    case e: Throwable => {
      log.error(s"Job failed with error: ${e.getMessage}", e)
      throw e
    }
  } finally {
    spark.stop()
  }
}
