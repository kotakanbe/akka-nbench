package bench.drivers

import bench._
import akka.actor._

import com.typesafe.config._

import dispatch._
import dispatch.Defaults._
//  import scala.util.{Success, Failure}

class HttpGetDriver(operation: String, stats: ActorRef, config: Config) extends Driver(operation, stats, config) {

  override val getOperation = () => {
    operation match {
      case "get" => get _
    }
  }

  def get(): (Boolean, Long, Long) = {
    val start = System.currentTimeMillis
    try {
      var status = false
      val request = url("http://localhost")
      val result = Http(request OK as.String).either
      result() match {
        case Right(content)         => status = true
        case Left(StatusCode(404))  => log.error("404 Not found")
        case Left(StatusCode(code)) => log.error("Some other code: " + code.toString)
        case e => log.error("unknown error: " + e)
      }

      val endAt = System.currentTimeMillis
      val elapsedMillis= endAt - start
      (status, endAt, elapsedMillis)
    } catch {
       case e: Throwable => {
         log.error("" + e)
         val endAt = System.currentTimeMillis
         val elapsedMillis= endAt - start
         (false, endAt, elapsedMillis)
       }
     }
  }
}

