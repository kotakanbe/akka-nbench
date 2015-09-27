package bench.drivers

import akka.actor._
import com.typesafe.config._

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util._
import org.apache.hadoop.hbase.HBaseConfiguration

class HBasePutDriver(operation: String, stats: ActorRef, config: Config) extends Driver(operation, stats, config) {

  var conn: Connection = _
  var table: Table = _
  val tableName= "ns:tbl"
  val colFamilies = List("fam")

  override val getOperation = () => {
    operation match {
      case "put" => putTest _
    }
  }

  override def setup(): Boolean = {
    val conf = HBaseConfiguration.create
    this.conn = ConnectionFactory.createConnection(conf)
    val admin = this.conn.getAdmin
    val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))

    try{
      colFamilies.foreach { fam=> tableDescriptor.addFamily(new HColumnDescriptor(fam)) }
      admin.createTable(tableDescriptor)
      log.info("table: ${tableName} created")
    } catch {
       case e: TableExistsException => {
         log.info(s"table: ${tableName} already exists")
       }
     }
    this.table = this.conn.getTable(TableName.valueOf(tableName))
    true
  }

  override def teardown(): Boolean = {
    this.table.close()
    this.conn.close()
    true
  }

  // https://github.com/hbasebook101/hbasebook101/blob/master/ch04/java-api-examples/src/main/java/example/PutExample.java
  // https://github.com/xldrx/hbase_examples/pull/1/files
  def putTest(): (Boolean, Long, Long) = {
    val start = System.currentTimeMillis

    try {
      val put = new Put(Bytes.toBytes("row-" + start ))
      put.addColumn(Bytes.toBytes("fam"), Bytes.toBytes("col1"), Bytes.toBytes("value1"))
      put.addColumn(Bytes.toBytes("fam"), Bytes.toBytes("col2"), 100L, Bytes.toBytes("value1"))
      table.put(put)

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

