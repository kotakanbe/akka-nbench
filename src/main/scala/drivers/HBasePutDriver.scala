package bench.drivers

import akka.actor._
import com.typesafe.config._

/*  import org.apache.hadoop.hbase.client.{HBaseAdmin,HTable,Put,Get} */
 /*  import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName} */
/*  import org.apache.hadoop.hbase.util.Bytes */

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util._
import org.apache.hadoop.hbase.client.{HBaseAdmin,Scan,Result}
import org.apache.hadoop.hbase.HBaseConfiguration

/*  import org.apache.hadoop.hbase.mapreduce.{TableMapper,TableMapReduceUtil} */
/*  import org.apache.hadoop.hbase.HBaseConfiguration */
/*  import org.apache.hadoop.hbase.io.ImmutableBytesWritable */

/*  import org.apache.hadoop.conf.Configuration */
/*  import org.apache.hadoop.mapreduce.Job */
/*  import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat */
/*  import org.apache.hadoop.io.{Text,NullWritable} */

class HBasePutDriver(operation: String, stats: ActorRef, config: Config) extends Driver(operation, stats, config) {

  var table: Table = _

  override val getOperation = () => {
    operation match {
      case "put" => putTest _
    }
  }

  override def setup(): Boolean = {
    val conf = HBaseConfiguration.create
    val conn = ConnectionFactory.createConnection(conf)
    this.table = conn.getTable(TableName.valueOf("ns:tbl"))
    true
  }

  override def teardown(): Boolean = {
    this.table.close()
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

