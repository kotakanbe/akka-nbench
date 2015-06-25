package bench

//
trait Tapper[A] {
  val obj:A
  def tap(f:A => Unit):A = { Option(obj).foreach(f);obj }
  def tapOption(f:A => Unit):Option[A] = { Option(obj).foreach(f);Option(obj) }
  def map[B](f:A => B):Option[B] = Option(obj).map(f)
  def toOption:Option[A] = Option(obj)
}

object Tapper {
  implicit def any2Tapper[A](a:A):Tapper[A] = new Tapper[A]{ val obj = a }
}
