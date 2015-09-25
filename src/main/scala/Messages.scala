package bench

import java.util.Date

case class OK()
case class Ready()
case class Go(doUntil: Date)
case class TearDown()
case class TearDownOnOnlyOneActor()

case class Result(ok: Boolean)
case class Stat(endAt: Long,  elapsedMillis: Long, operation: String, ok: Boolean) {
  override def toString: String = {
    s"$endAt,$elapsedMillis,$operation,$ok"
  }
}

