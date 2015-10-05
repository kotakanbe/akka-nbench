// HBase徹底入門 Chapter7 メッセージングサービスのScala版 HBase1.x系で実装
// http://amzn.to/1Gn5Azr
// https://github.com/hbasebook101/hbasebook101/tree/master/ch07/messaging-service/src/main/java/messaging 
package bench.drivers

import akka.actor._
import com.typesafe.config._

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util._
import org.apache.hadoop.hbase.HBaseConfiguration

import org.apache.hadoop.hbase.util.Hash
import org.apache.hadoop.hbase.types._

import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.FilterList.Operator
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp

import scala.collection.JavaConversions._

import java.text.SimpleDateFormat
//  import java.util.ArrayList
import java.util.Calendar
import java.util.Date
//  import java.util.List
//  import java.util.Map.Entry

class HBaseAccessCounter(operation: String, stats: ActorRef, config: Config) extends Driver(operation, stats, config) {

  override val getOperation = () => {
    operation match {
      case "count" => count _
    }
  }

  var conn: Connection = _
  var table: Table = _
  val nsName = "ns"
  val tableName= s"${nsName}:access"
  val colFamily = List("d", "h")

  var hash = Hash.getInstance(Hash.MURMUR_HASH3)

  var urlRowSchema = 
    new StructBuilder().
    add(new RawInteger()). // 
    add(new RawStringTerminated(Array[Byte]( 0x00 ))). //
    add(RawString.ASCENDING).
    toStruct()

    var domainRowSchema = 
      new StructBuilder().
      add(new RawInteger()). // 
      add(RawString.ASCENDING).
      toStruct()

      override def setup(): Boolean = {
        val conf = HBaseConfiguration.create
        this.conn = ConnectionFactory.createConnection(conf)
        val admin = this.conn.getAdmin

        try{
          admin.createNamespace(NamespaceDescriptor.create(nsName).build());
        } catch {
          case e: NamespaceExistException => {
            log.info(s"namespace: ${nsName} already exists")
          }
        }

        val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
        try{
          colFamily.foreach{ c => tableDescriptor.addFamily(new HColumnDescriptor(c)) }
          admin.createTable(tableDescriptor)
          log.info(s"table: ${tableName} created")
        } catch {
          case e: TableExistsException => {
            log.info(s"table: ${tableName} already exists")
          }
        }
        this.table = this.conn.getTable(TableName.valueOf(tableName))
        true
      }

      def count(): (Boolean, Long, Long) = {
        Thread.sleep(100)
        val start = System.currentTimeMillis
        try {
          countup("blog", "yahoo.co.jp", s"test1", 1)

          val endAt = System.currentTimeMillis
          val elapsedMillis= endAt - start
          (true, endAt, elapsedMillis)
        } catch {
          case e: Throwable => {
            log.error("" + e)
            log.error("" + e.getMessage())
            e.printStackTrace
            val endAt = System.currentTimeMillis
            val elapsedMillis= endAt - start
            (false, endAt, elapsedMillis)
          }
        }
      }

      def countup(subDomain: String, rootDomain: String, path: String, amount: Long) = {
        val domain = subDomain + "." + rootDomain;
        val reversedDomain = reverseDomain(domain);

        val date = new Date()
        val hourlyFormat = new SimpleDateFormat("yyyyMMddHH");
        val dailyFormat = new SimpleDateFormat("yyyyMMddHH");

        val increments = new java.util.ArrayList[Row]()
        
        // URL
        val urlRow = createURLRow(domain, path)
        val urlIncrement = new Increment(urlRow)
        urlIncrement.addColumn(Bytes.toBytes("h"), Bytes.toBytes(hourlyFormat.format(date)), amount)
        urlIncrement.addColumn(Bytes.toBytes("d"), Bytes.toBytes(dailyFormat.format(date)), amount)
        increments.add(urlIncrement)
        

        // Domain
        val domainRow = createDomainRow(rootDomain, reversedDomain)
        val domainIncrement = new Increment(domainRow)
        domainIncrement.addColumn(Bytes.toBytes("h"), Bytes.toBytes(hourlyFormat.format(date)), amount)
        domainIncrement.addColumn(Bytes.toBytes("d"), Bytes.toBytes(dailyFormat.format(date)), amount)
        increments.add(domainIncrement)


        val results = new Array[AnyRef](2)
        this.table.batch(increments, results)
      }


  def createDomainRow(rootDomain: String, reversedDomain: String): Array[Byte] = {
    val values = Array[AnyRef] (  
      hash.hash(Bytes.toBytes(rootDomain)).asInstanceOf[AnyRef], 
      reversedDomain
    )
    val positionedByteRange = new SimplePositionedMutableByteRange(domainRowSchema.encodedLength(values))
    domainRowSchema.encode(positionedByteRange, values)
    positionedByteRange.getBytes()
  }
     
  def createURLRow(domain: String, path: String): Array[Byte] = {
    val values = Array[AnyRef] ( 
      hash.hash(Bytes.toBytes(domain)).asInstanceOf[AnyRef], 
      domain, 
      path
    )
    val positionedByteRange = new SimplePositionedMutableByteRange(urlRowSchema.encodedLength(values))
    urlRowSchema.encode(positionedByteRange, values)
    positionedByteRange.getBytes()
  }

  def reverseDomain(domain: String): String = {
    domain.split(".").reverse.mkString(".")
  }
}

case class DomainAccesscount(
  domain: String,
  time: Calendar,
  count: Long
) {
  override def toString: String = {
    s"domain:\t${domain}, time: ${time}, count: ${count}"
  }
}

case class URLAccessCount(
  domain: String,
  path: String,
  time: Calendar,
  count: Long
) {
  override def toString: String = {
    s"domain:\t${domain}, path:${path}, time: ${time}, count: ${count}"
  }
}

