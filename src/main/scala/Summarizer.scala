package bench

import com.github.tototoshi.csv._
import com.github.tototoshi.csv._
import org.apache.commons.math3.stat.StatUtils

import java.nio.file._
import java.io._

abstract class StatBase {
  def elapsed: Int
}

case class SummaryStat(
  elapsed: Int,
  window: Int,
  total: Int,
  successful: Int,
  failed: Int
) extends StatBase {
  def toList = List(elapsed, window, total ,successful, failed).map(_.toString)
}

case class LatenciesStat(
  elapsed: Int,
  window: Int,
  n: Int,
  min: Long,
  mean: Long,
  median: Long,
  percentile95th: Long,
  percentile99th: Long,
  percentile99_9th: Long,
  max: Long,
  errors: Int
) extends StatBase {
  def toList = List(
    elapsed,
    window,
    n,
    min,
    mean,
    median,
    percentile95th,
    percentile99th,
    percentile99_9th,
    max,
    errors
  ).map(_.toString)
}

//  object Summarizer {
object Summarizer extends App with Utils {

  def csvFieldsToStat(fields: Seq[String]): Stat = {
    fields match {
      case List(endAt: String, elapsedMillis: String, operation: String, ok: String) =>
        try {
          Stat(endAt.toLong, elapsedMillis.toLong, operation, ok.toBoolean)
        } catch {
          case e: Exception => throw UnknownCSVFormatException(s"$fields")
        }
      case _ =>
        println("unknown format: " + fields)
        throw UnknownCSVFormatException(s"$fields")
    }
  }

  def writeToCSV(path:String, allLines: List[List[String]]) = {
    val w = CSVWriter.open(new File(path))
    w.writeAll(allLines)
    w.close()
  }

  var thisWindowStats: List[Stat] = Nil
  var oldest = Long.MaxValue

  //TODO reportInterval
  val reportInterval = 1
  //  val reportInterval = 10
  val reportIntervalMillis = reportInterval * 1000 //milliseconds

  var nextWindowStartAt = Long.MinValue

  //  var totalSummaryStats: List[List[String]] = Nil
  var summaryStats = collection.mutable.Map.empty[String, List[SummaryStat]]
  var latenciesStats = collection.mutable.Map.empty[String, List[LatenciesStat]]

  val csvRawPath = FileSystems.getDefault().getPath("tests/current/raw.csv")
  val reader = CSVReader.open(csvRawPath.toFile)
  reader.foreach { fields =>

    val stat: Stat = csvFieldsToStat(fields)

    // initialize start times at 1st Row
    if (stat.endAt < oldest) {
      oldest = stat.endAt
      nextWindowStartAt = oldest + reportIntervalMillis
    }

    if (nextWindowStartAt < stat.endAt) {

      // caluculate this window stats because it found a next window's record.
      thisWindowStats.groupBy(_.operation).foreach { case (operation, l) =>
        val nthWindow = ((nextWindowStartAt - oldest) / reportIntervalMillis).toInt
        val elapsed = nthWindow * reportInterval
        val total = l.length
        val ok = l.count( _.ok )
        val ng = total - ok

        // summary.r expect in Microsecs
        val elapsedMicros = l.map(_.elapsedMillis.toDouble * 1000).toArray

        // for operation_summary.csv
        val summaryStat = SummaryStat(
          reportInterval * nthWindow, reportInterval, total, ok, ng
        )
        this.summaryStats(operation) = this.summaryStats.get(operation) match {
          case Some(l) => summaryStat :: l
          case _ => summaryStat :: Nil
        }

        // for operation_latencies.csv
        val latenciesStat = LatenciesStat(
          reportInterval * nthWindow,
          reportInterval,
          l.length,
          elapsedMicros.min.toLong,
          StatUtils.mean(elapsedMicros).toLong,
          StatUtils.percentile(elapsedMicros, 50.0).toLong,
          StatUtils.percentile(elapsedMicros, 95.0).toLong,
          StatUtils.percentile(elapsedMicros, 99.0).toLong,
          StatUtils.percentile(elapsedMicros, 99.9).toLong,
          elapsedMicros.max.toLong,
          ng
        )
        this.latenciesStats(operation) = this.latenciesStats.get(operation) match {
          case Some(l) => latenciesStat :: l
          case _ => latenciesStat:: Nil
        }
      }

      // slide to Next Window
      thisWindowStats = stat :: Nil
      val nEmptyWinwodws = (stat.endAt - nextWindowStartAt) / reportIntervalMillis
      nextWindowStartAt += (reportIntervalMillis * nEmptyWinwodws + reportIntervalMillis)

    } else {
      thisWindowStats = stat :: thisWindowStats
    }
  }

  val csvSymlink = FileSystems.getDefault().getPath("tests/current").toAbsolutePath

  this.summaryStats.foreach { case (key, missingTeethStats) =>
    var fullTeethStats: List[SummaryStat] = Nil
    missingTeethStats.reverse.sliding(2).foreach { case List(x, y) =>
      if(fullTeethStats.isEmpty && x.elapsed != reportInterval) {
        fullTeethStats = x :: (reportInterval until x.elapsed by reportInterval).toList.map(
          SummaryStat(_, 0, 0, 0, 0)
        ).reverse
      } else if(y.elapsed - x.elapsed != reportInterval) {
        fullTeethStats = (x.elapsed + reportInterval until y.elapsed by reportInterval).toList.map(
          SummaryStat(_, 0, 0, 0, 0)
        ).reverse ::: List(x) ::: fullTeethStats
      } else {
        fullTeethStats = x :: fullTeethStats
      }
    }
    this.summaryStats(key) = fullTeethStats.reverse
  }

  val summaryHeader = List("elapsed", "window", "total", "successful", "failed")
  this.summaryStats.foreach { case (key, l) =>
    writeToCSV(pathJoin(csvSymlink.toString, key + "_summary.csv"),
      summaryHeader :: l.map(_.toList))
  }

  val totalSummaryStats = this.summaryStats.values.toList.flatten.groupBy(_.elapsed).map{ case (k, v) =>
    v.foldLeft(SummaryStat(k, 0, 0, 0, 0)) { (x, y) =>
      SummaryStat(
        k,
        reportInterval,
        x.total + y.total,
        x.successful + y.successful,
        x.failed + y.failed)
    }
  }.toList.sortBy(_.elapsed)
  writeToCSV(pathJoin(csvSymlink.toString, "summary.csv"),
    summaryHeader :: totalSummaryStats.map(_.toList))

  this.latenciesStats.foreach { case (key, missingTeethStats) =>
    var fullTeethStats: List[LatenciesStat] = Nil
    missingTeethStats.reverse.sliding(2).foreach { case List(x, y) =>
      if(fullTeethStats.isEmpty && x.elapsed != reportInterval) {
        fullTeethStats = x :: (reportInterval until x.elapsed by reportInterval).toList.map(
          LatenciesStat(_, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        ).reverse
      } else if(y.elapsed - x.elapsed != reportInterval) {
        fullTeethStats = (x.elapsed + reportInterval until y.elapsed by reportInterval).toList.map(
          LatenciesStat(_, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        ).reverse ::: List(x) ::: fullTeethStats
      } else {
        fullTeethStats = x :: fullTeethStats
      }
    }
    this.latenciesStats(key) = fullTeethStats.reverse
  }
  this.latenciesStats.foreach { case (key, l) =>
    val header = List("elapsed", "window", "n", "min", "mean", "median", "95th", "99th", "99_9th", "max", "errors")
    writeToCSV(pathJoin(csvSymlink.toString, key + "_latencies.csv"),
      header :: l.map(_.toList))
  }
}

