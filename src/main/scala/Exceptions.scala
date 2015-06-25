package bench

// TODO These Case Exceptions move to Other File
case class ApplicationConfigException(msg: String) extends Exception
case class UnknownCSVFormatException(msg: String) extends Exception
case class UnknownMessageException(msg: String) extends Exception
case class NotASymbolicLinkException(msg: String) extends Exception

