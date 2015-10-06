// HBase徹底入門 Chapter7 メッセージングサービスのScala版 HBase1.x系で実装
// http://amzn.to/1Gn5Azr
// https://github.com/hbasebook101/hbasebook101/tree/master/ch07/document-version-management-system 
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

class HBaseDocumentVersion(operation: String, stats: ActorRef, config: Config) extends Driver(operation, stats, config) {

  override val getOperation = () => {
    operation match {
      case "save" => save _
      case "list" => list _
    }
  }

  var conn: Connection = _
  var table: Table = _
  val nsName = "ns"
  val tableName= s"${nsName}:document"
  val colFamily = "d"

  var hash = Hash.getInstance(Hash.MURMUR_HASH3)

  var documentRowSchema = 
    new StructBuilder().
    add(new RawInteger). // 
    add(RawString.ASCENDING).toStruct


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

    // http://archive.cloudera.com/cdh5/cdh/5/hbase/apidocs/org/apache/hadoop/hbase/HColumnDescriptor.html
    val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
    try{
      tableDescriptor.addFamily(
        new HColumnDescriptor(colFamily).
        setMaxVersions(100).
        setBloomFilterType(org.apache.hadoop.hbase.regionserver.BloomType.ROW)
      ) 
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

  def save(): (Boolean, Long, Long) = {
    Thread.sleep(300)
    val start = System.currentTimeMillis
    try {
      
      saveDocument("doc1", "test1", s"hello_${start}");

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

  def saveDocument(documentId: String, title: String, text: String): Unit = {
    val row = createDocumentRow(documentId)
    while (true) {
      val get = new Get(row)
      get.addColumn(Bytes.toBytes("d"), Bytes.toBytes("ver"))
      val verResult = table.get(get)

      var version: Long = 0
      var oldVersionBytes: Array[Byte] = null
      if (verResult.isEmpty) {
        oldVersionBytes = HConstants.EMPTY_BYTE_ARRAY
        version = 1
      } else {
        oldVersionBytes = verResult.getValue(Bytes.toBytes("d"), Bytes.toBytes("ver"))
        val oldVersion = Bytes.toLong(oldVersionBytes)
        version = oldVersion + 1
      }

      val put = new Put(row, version)
      put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("ver"), Bytes.toBytes(version))
      put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("title"), Bytes.toBytes(title))
      put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("text"), Bytes.toBytes(text))

      val success =
          table.checkAndPut(row, Bytes.toBytes("d"), Bytes.toBytes("ver"), oldVersionBytes, put)
      if (success) {
        return
      }
    }
  }

  def list(): (Boolean, Long, Long) = {
    Thread.sleep(1000)
    val start = System.currentTimeMillis
    try {
      
      val vers = listVersions("doc1")
      vers.foreach{ v => log.info(""+v)}

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

  def listVersions(documentId: String): ArrayList[Long] = {
    val row = createDocumentRow(documentId)
    val get = new Get(row)
    get.addColumn(Bytes.toBytes("d"), Bytes.toBytes("ver"))
    get.setMaxVersions

    val versions = new ArrayList[Long]()
    val result = this.table.get(get)
    result.getMap.get(Bytes.toBytes("d")).get(Bytes.toBytes("ver")).keySet.foreach{ version =>
      versions.add(version)
    }
    return versions
  }

  def createDocumentRow(documentId: String): Array[Byte] = {
    val values = Array[AnyRef] (
      hash.hash(Bytes.toBytes(documentId)).asInstanceOf[AnyRef],
      documentId
    )
    val positionedByteRange = new SimplePositionedMutableByteRange(
      documentRowSchema.encodedLength(values))
    documentRowSchema.encode(positionedByteRange, values)
    positionedByteRange.getBytes
  }
}

//  case class URLAccessCount(
//    domain: String,
//    path: String,
//    time: Calendar,
//    count: Long
//  ) {
//    override def toString: String = {
//      val sdf = new SimpleDateFormat("yyyyMMddHH")
//      s"domain: ${domain}, path:${path}, time: ${sdf.format(time.getTime)}, count: ${count}"
//    }
//  }

