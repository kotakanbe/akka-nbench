package bench

trait Utils {

  // http://d.hatena.ne.jp/sudix/20110823/1314085323
  def pathJoin(pathes: String*): String = {
    import java.io.File
    pathes.foldLeft(""){(s, p) => (new File(s, p)).getAbsolutePath()}
  }

  // http://stackoverflow.com/questions/4604237/how-to-write-to-a-file-in-scala
  def using(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() } 
  }


  def using(s: scala.io.Source)(f: scala.io.Source=>Any) {
    try { f(s) } finally { s.close() }
  }
}
