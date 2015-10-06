// HBase徹底入門 Chapter7 メッセージングサービスのScala版 HBase1.x系で実装
// http://amzn.to/1Gn5Azr
// https://github.com/hbasebook101/hbasebook101/tree/master/ch07/access-counter-service
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
import org.apache.hadoop.hbase.filter.ColumnRangeFilter

import scala.collection.JavaConversions._

import java.text.SimpleDateFormat
import java.util.ArrayList
import java.util.Calendar
import java.util.Date
//  import java.util.List
//  import java.util.Map.Entry

class HBaseAccessCounter(operation: String, stats: ActorRef, config: Config) extends Driver(operation, stats, config) {

  override val getOperation = () => {
    operation match {
      case "count" => count _
      case "URLHourly" => hourlyURLAccessCount _
      case "DomainDaily" => dailyDomainAccessCount _
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
    add(new RawInteger). // 
    add(new RawStringTerminated(Array[Byte]( 0x00 ))). //
    add(RawString.ASCENDING).
    toStruct

  var domainRowSchema = 
    new StructBuilder().
    add(new RawInteger). // 
    add(RawString.ASCENDING).
    toStruct

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
        countup("blog", "yahoo.co.jp", "test1", 1)

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
      //TODO original : yyyyMMddHH
      val dailyFormat = new SimpleDateFormat("yyyyMMdd");

      val increments = new ArrayList[Row]

      // URL
      val urlRow = createURLRow(domain, path)
      val urlIncrement = new Increment(urlRow)
      urlIncrement.addColumn(
        Bytes.toBytes("h"), 
        Bytes.toBytes(hourlyFormat.format(date)), 
        amount
      )
      urlIncrement.addColumn(
        Bytes.toBytes("d"), 
        Bytes.toBytes(dailyFormat.format(date)), 
        amount
      )
      increments.add(urlIncrement)


      // Domain
      val domainRow = createDomainRow(rootDomain, reversedDomain)
      val domainIncrement = new Increment(domainRow)
      domainIncrement.addColumn(
        Bytes.toBytes("h"), 
        Bytes.toBytes(hourlyFormat.format(date)), 
        amount
      )
      domainIncrement.addColumn(
        Bytes.toBytes("d"), 
        Bytes.toBytes(dailyFormat.format(date)), 
        amount
      )
      increments.add(domainIncrement)

      val results = new Array[AnyRef](2)
      this.table.batch(increments, results)
    }

    def hourlyURLAccessCount(): (Boolean, Long, Long) = {
      Thread.sleep(1000)
      val start = System.currentTimeMillis
      try {

        val calendar = Calendar.getInstance
        calendar.setTime(new Date)
        val result = getHourlyURLAccessCount(
          "blog",
          "yahoo.co.jp",
          "test1",
          calendar,
          calendar
        )
        result.foreach{ r => log.info("HourlyURL: "+r) }

        val endAt = System.currentTimeMillis
        val elapsedMillis= endAt - start
        (true, endAt, elapsedMillis)
      } catch {
        case e: Throwable => {
          log.error("" + e)
          log.error("" + e.getMessage)
          e.printStackTrace
          val endAt = System.currentTimeMillis
          val elapsedMillis= endAt - start
          (false, endAt, elapsedMillis)
        }
      }
    }

    def getHourlyURLAccessCount(subDomain: String, rootDomain: String,
      path: String, startHour: Calendar, endHour: Calendar): ArrayList[URLAccessCount] = {
        val domain = subDomain + "." + rootDomain
        val row = createURLRow(domain, path)
        val get = new Get(row)
        get.addFamily(Bytes.toBytes("h"))

        val sdf = new SimpleDateFormat("yyyyMMddHH");
        get.setFilter(
          new ColumnRangeFilter(
            Bytes.toBytes(sdf.format(startHour.getTime())), 
            true, 
            Bytes.toBytes(sdf.format(endHour.getTime())), 
            true
          ))

        val ret = new ArrayList[URLAccessCount]
        val result = this.table.get(get)
        if (result.isEmpty) {
          return ret
        }

        result.getFamilyMap(Bytes.toBytes("h")).entrySet.foreach { entry =>
          val yyyyMMddHH = entry.getKey
          val value = entry.getValue
          val time = Calendar.getInstance
          time.setTime(sdf.parse(Bytes.toString(yyyyMMddHH)))
          val accessCount = URLAccessCount(
            domain,
            path,
            time,
            Bytes.toLong(value)
          )
          ret.add(accessCount)
        }
        return ret
    }

    def dailyDomainAccessCount(): (Boolean, Long, Long) = {
      Thread.sleep(1000)
      val start = System.currentTimeMillis
      try {

        val calendar = Calendar.getInstance
        calendar.setTime(new Date)
        val result = getDailyDomainAccessCount(
          "yahoo.co.jp",
          calendar,
          calendar
        )
        result.foreach{ r => log.info("DailyDomain: "+r) }

        val endAt = System.currentTimeMillis
        val elapsedMillis= endAt - start
        (true, endAt, elapsedMillis)
      } catch {
        case e: Throwable => {
          log.error("" + e)
          log.error("" + e.getMessage)
          e.printStackTrace
          val endAt = System.currentTimeMillis
          val elapsedMillis= endAt - start
          (false, endAt, elapsedMillis)
        }
      }
    }

    def getDailyDomainAccessCount(rootDomain: String, startDay: Calendar,
      endDay: Calendar): ArrayList[DomainAccessCount] = {

        val reversedRootDomain = reverseDomain(rootDomain)
        val startRow = createDomainRow(rootDomain, reversedRootDomain)
        val stopRow = incrementBytes(createDomainRow(rootDomain, reversedRootDomain))
        val scan = new Scan(startRow, stopRow)
        scan.addFamily(Bytes.toBytes("d"))

        val sdf = new SimpleDateFormat("yyyyMMdd")
        scan.setFilter(new ColumnRangeFilter(
          Bytes.toBytes(sdf.format(startDay.getTime)), 
          true,
          Bytes.toBytes(sdf.format(endDay.getTime)), 
          true
        ))

        val ret = new ArrayList[DomainAccessCount]
        val scanner = this.table.getScanner(scan)
        scanner.foreach{ result =>
          val domain = reverseDomain(
            domainRowSchema.decode(
              new SimplePositionedByteRange(result.getRow()), 1).toString
            )
          result.getFamilyMap(Bytes.toBytes("d")).entrySet().foreach { entry =>
            val qualifier = entry.getKey
            val value = entry.getValue
            val time = Calendar.getInstance
            time.setTime(sdf.parse(Bytes.toString(qualifier)))
            val accessCount = DomainAccessCount(
              reverseDomain(domain),
              time,
              Bytes.toLong(value)
            )
            ret.add(accessCount)
          }
        }
        return ret
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
    domain.split('.').reverse.mkString(".")
  }

  def incrementBytes(bytes: Array[Byte]): Array[Byte] = {
    for ((_, i) <- bytes.zipWithIndex) {
      var increase = false
      val value = bytes(bytes.length - (i + 1)) & 0x0ff
      var total: Int = value + 1
      if (total > 255) {
        increase = true
        total = 0
      }
      bytes(bytes.length - (i + 1)) = total.asInstanceOf[Byte]
      if (!increase) {
        return bytes
      }
    }
    return bytes
  }
}

case class DomainAccessCount(
  domain: String,
  time: Calendar,
  count: Long
) {
  override def toString: String = {
    val sdf = new SimpleDateFormat("yyyyMMddHH")
    s"domain: ${domain}, time: ${sdf.format(time.getTime)}, count: ${count}"
  }
}

case class URLAccessCount(
  domain: String,
  path: String,
  time: Calendar,
  count: Long
) {
  override def toString: String = {
    val sdf = new SimpleDateFormat("yyyyMMddHH")
    s"domain: ${domain}, path:${path}, time: ${sdf.format(time.getTime)}, count: ${count}"
  }
}

