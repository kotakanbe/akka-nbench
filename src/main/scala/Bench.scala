package bench

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._
import scala.reflect.runtime.universe._
import scala.concurrent.Await

import com.typesafe.config._
import net.ceedubs.ficus.Ficus._

import java.util.Properties
import java.nio.file._
import java.util.Date
import java.text.SimpleDateFormat
import java.util.Date

import Tapper._

object Bench extends App {

  def prepareOutputDirs(): String = {
    val csvDateTimeDir = FileSystems.getDefault().getPath(
      "tests/" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()))
    Files.createDirectories(csvDateTimeDir)
    val csvSymlink = FileSystems.getDefault().getPath("tests/current")
    if(Files.isSymbolicLink(csvSymlink)){
      Files.delete(csvSymlink)
    } else if (Files.exists(csvSymlink)) {
      throw new NotASymbolicLinkException(s"test/current is not a symbolic link. Path: $csvSymlink")
    }
    Files.createSymbolicLink(csvSymlink, csvDateTimeDir.toAbsolutePath)
    csvDateTimeDir.toAbsolutePath.toString
  }

  def parseOptions(): String = {
     val usage = """
         Usage: activator -mem 4096 "run-main bench.Bench scenario_name"
     """
    if (args.length != 1) println(usage)
    return args(0)
  }

  val scenario = parseOptions
  val config = ConfigFactory.load().getConfig(scenario)
  val duration = config.getInt("duration")
  val concurrent = config.getInt("concurrent")
  val csvDateTimeDir = prepareOutputDirs

  val system = ActorSystem("bench")
  val actorProps = Props(classOf[StatsCollector], csvDateTimeDir, config)
  val statsCollector = system.actorOf(actorProps, name = "statscollector")

  val operationsWithRatio: Map[String, Int] = config.as[Map[String, Int]]("operations")
  val numer = operationsWithRatio.values.sum
  if (concurrent < numer){
    val msg = s"concurrent($concurrent) must greater than sum of operations ratio($numer)"
    System.err.println(msg)
    throw new ApplicationConfigException(msg)
  }
  val operations = for((key, value) <- operationsWithRatio) yield {
    List.range(0, concurrent * operationsWithRatio(key) / numer).map(_ => key)
  }

  implicit val timeout = Timeout(duration * 2, SECONDS)
  var driverClz = Class.forName(config.getString("driver"))
  val drivers = operations.flatten.zipWithIndex.map{ case (operation, i) =>
    system.actorOf(Props(driverClz, operation, statsCollector, config).withDispatcher("my-dispatcher"), name = s"driver_$i")
  }

  drivers.par.map(actor => actor ? Ready()).foreach{ f =>
    Await.result(f, timeout.duration).asInstanceOf[OK]
  }

  val startAt = new Date()
  val doUntil = new Date(startAt.getTime + duration * 1000)
  drivers.par.map(actor => actor ? Go(doUntil)).foreach { f =>
    Await.result(f, timeout.duration).asInstanceOf[OK]
  }

  (statsCollector ? TearDown()).tap { f =>
    Await.result(f, timeout.duration).asInstanceOf[OK]
  }

  drivers.par.map(actor => actor ? TearDown()).foreach { f =>
    Await.result(f, timeout.duration).asInstanceOf[OK]
  }

  (drivers.head ? TearDown()).tap { f =>
    Await.result(f, timeout.duration).asInstanceOf[OK]
  }

  system.awaitTermination()
}

