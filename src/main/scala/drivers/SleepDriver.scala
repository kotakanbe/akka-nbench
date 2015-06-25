package bench.drivers

import bench._
import akka.actor._

import com.typesafe.config._

class SleepDriver(operation: String, stats: ActorRef, config: Config) extends Driver(operation, stats, config) {

  override val getOperation = () => {
    operation match {
      case "sleep1" => sleep1 _
      case "sleep2" => sleep2 _
    }
  }

  def sleep1(): (Boolean, Long, Long) = {
    val start = System.currentTimeMillis
    try {
      log.info("sleep1")
      //  Thread.sleep(1000)
      Thread.sleep(100)
      val endAt = System.currentTimeMillis
      val elapsedMillis= endAt - start
      (true, endAt, elapsedMillis)
    } catch {
       case e: Throwable => {
         log.error("" + e)
         val endAt = System.currentTimeMillis
         val elapsedMillis= endAt - start
         (false, endAt, elapsedMillis)
       }
     }
  }

  def sleep2(): (Boolean, Long, Long) = {
    val start = System.currentTimeMillis
    try {
      log.info("sleep2")
      Thread.sleep(1000)
      val endAt = System.currentTimeMillis
      val elapsedMillis= endAt - start
      (true, endAt, elapsedMillis)
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

