package bench.drivers

import akka.actor._
import com.typesafe.config._

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util._
import org.apache.hadoop.hbase.HBaseConfiguration

import org.apache.hadoop.hbase.util.Hash
import org.apache.hadoop.hbase.types._

class HBaseMessagingServiceDriver(operation: String, stats: ActorRef, config: Config) extends Driver(operation, stats, config) {

  override val getOperation = () => {
    operation match {
      case "send" => send _
    }
  }

  var conn: Connection = _
  var table: Table = _
  val nsName = "ns"
  val tableName= s"${nsName}:message"
  val colFamily = "m"

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
      tableDescriptor.addFamily(new HColumnDescriptor(colFamily))
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

  def send(): (Boolean, Long, Long) = {
    val start = System.currentTimeMillis
    try {
      Thread.sleep(500)
      sendMessage(1, start, "hoge hoge.")
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

  def sendMessage(roomId: Long, userId: Long, body: String) = {
    val postAt = System.currentTimeMillis()
    val messageId = java.util.UUID.randomUUID().toString()
    val row = createMessageRow(roomId, postAt, messageId)

    val put = new Put(row)
    put.addColumn(Bytes.toBytes("m"), Bytes.toBytes("messageId"), Bytes.toBytes(messageId))
    put.addColumn(Bytes.toBytes("m"), Bytes.toBytes("userId"), Bytes.toBytes(userId))
    put.addColumn(Bytes.toBytes("m"), Bytes.toBytes("body"), Bytes.toBytes(body))

    this.table.put(put)
  }

  def createMessageRow(roomId: Long, postAt: Long, messageId: String): Array[Byte] = {
    var hash = Hash.getInstance(Hash.MURMUR_HASH3)
    var values = Array[Object](
      hash.hash(Bytes.toBytes(roomId)).toLong.asInstanceOf[Object],
      roomId.asInstanceOf[Object],
      (Long.MaxValue - postAt).asInstanceOf[Object],
      messageId.asInstanceOf[String]
    )
    var messageRowSchema = 
      new StructBuilder().
        add(new RawLong()).
        add(new RawLong()).
        add(new RawLong()).
        add(RawString.ASCENDING).
        toStruct()

    var positionedByteRange = 
      new SimplePositionedMutableByteRange(messageRowSchema.encodedLength(values))
    messageRowSchema.encode(positionedByteRange, values)
    positionedByteRange.getBytes()
  }
}

