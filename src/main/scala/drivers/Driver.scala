package bench.drivers

import bench._
import akka.actor._

import com.typesafe.config._

import java.util.Date

class Driver(operation: String, stats: ActorRef, config: Config) extends Actor with ActorLogging {

  val getOperation = () => {
    run _
  }

  def setup(): Boolean = {
    log.debug("setup")
    true
  }

  def run(): (Boolean, Long, Long) = {
    val start = System.currentTimeMillis
    Thread.sleep(1000)
    log.info("Dummy Driver")
    val endAt = System.currentTimeMillis
    val elapsedMillis= endAt - start
    (true, endAt, elapsedMillis)
  }

  def teardown(): Boolean = {
    log.debug("teardown")
    true
  }

  def teardownExecutedOnOneActor(): Boolean = {
    log.debug("teardownExecutedOnOneActor")
    true
  }

  def unlessTimeup(doUntil: Date): Boolean = { 
    new Date().compareTo(doUntil) < 1
  }

  def receive = {
    case Ready() =>
      setup()
      sender ! OK()

    case Go(doUntil) =>
      log.info(self.path.name + ": " + operation + " starting...")
      while(unlessTimeup(doUntil)) {
        val (ok, endAt, elapsedMillis) = getOperation()()
        this.stats ! Stat(endAt, elapsedMillis, operation, ok)
      }
      sender ! OK()

    case TearDown() =>
      teardown()
      sender ! OK()

    case TearDownExecutedOnOneActor() =>
      teardownExecutedOnOneActor()
      sender ! OK()

    case m =>
      throw new UnknownMessageException("Unknown Message: " + m)
  }
}

