package bench

import akka.actor._

import com.typesafe.config._

import java.util.Date
import java.io._

import Tapper._

class StatsCollector(csvdir: String, config: Config) extends Actor with ActorLogging  with Utils {

  val csvPath = pathJoin(csvdir, "raw.csv")
  val p = new PrintWriter(new BufferedWriter(new FileWriter(new File(csvPath))));
  var count = 0

  def receive = {
    case s: Stat =>
      p.println(s)
      count += 1
      if(count % 1000 == 0) {
        log.debug(s"proccessed $count")
      }
    case TearDown() =>
      log.info(s"total requests: $count")
      log.info(s"dumped csv to $csvPath")

      p.flush
      p.close

      val configPath = pathJoin(csvdir, "config.txt")
      using(new File(configPath)) { p =>
        p.println(config.toString)
      }

      sender ! OK()
  }
}

