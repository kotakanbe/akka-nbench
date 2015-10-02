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

class HBaseMessagingServiceDriver(operation: String, stats: ActorRef, config: Config) extends Driver(operation, stats, config) {

  override val getOperation = () => {
    operation match {
      case "send" => send _
      case "initial" => initial _
    }
  }

  var conn: Connection = _
  var table: Table = _
  val nsName = "ns"
  val tableName= s"${nsName}:message"
  val colFamily = "m"

  var hash = Hash.getInstance(Hash.MURMUR_HASH3)

  var messageRowSchema = 
    new StructBuilder().
    add(new RawLong()).
    add(new RawLong()).
    add(new RawLong()).
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
        Thread.sleep(100)
        val userId = scala.util.Random.nextInt(10)
        sendMessage(1, userId, s"Hello I am ${userId}.")
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

      def createMessageRow(roomId: Long, postAt: Long, messageId: String): Array[Byte] = {
        var values = Array[AnyRef](
          hash.hash(Bytes.toBytes(roomId)).toLong.asInstanceOf[AnyRef],
          roomId.asInstanceOf[AnyRef],
          (Long.MaxValue - postAt).asInstanceOf[AnyRef],
          messageId.asInstanceOf[String]
        )

        var positionedByteRange = 
          new SimplePositionedMutableByteRange(messageRowSchema.encodedLength(values))
        messageRowSchema.encode(positionedByteRange, values)
        positionedByteRange.getBytes()
      }

      val postAt = System.currentTimeMillis()
      val messageId = java.util.UUID.randomUUID().toString()
      val row = createMessageRow(roomId, postAt, messageId)

      val put = new Put(row)
      put.addColumn(Bytes.toBytes("m"), Bytes.toBytes("messageId"), Bytes.toBytes(messageId))
      put.addColumn(Bytes.toBytes("m"), Bytes.toBytes("userId"), Bytes.toBytes(userId))
      put.addColumn(Bytes.toBytes("m"), Bytes.toBytes("body"), Bytes.toBytes(body))

      this.table.put(put)
    }

    def initial(): (Boolean, Long, Long) = {
      val start = System.currentTimeMillis
      try {
        Thread.sleep(500)
        val msgs = getInitialMessages(1, List(0))
        log.info(s"num msgs ${msgs.length}")
        //  msgs.foreach {msg => log.info(msg.body) }
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

    def getInitialMessages(roomId: Long, blockUsers: List[Long]): List[Message] = {
      val startRow = createMessageScanRow(roomId)
      val stopRow = incrementBytes(createMessageScanRow(roomId))

      val scan = new Scan(startRow, stopRow)

      val filterList = new FilterList(Operator.MUST_PASS_ALL);
      if (blockUsers != null) {
        blockUsers.foreach { userId => 
          val userFilter =
            new SingleColumnValueFilter(Bytes.toBytes("m"), Bytes.toBytes("userId"),
              CompareOp.NOT_EQUAL, Bytes.toBytes(userId))
          filterList.addFilter(userFilter)
        }
      }

      scan.setFilter(filterList)
      val messages = scala.collection.mutable.ListBuffer.empty[Message]
      val scanner: ResultScanner = this.table.getScanner(scan)
      var count = 0

      import scala.collection.JavaConversions._
      scanner.iterator.foreach { result =>
        messages.append(convertToMessage(result))
        count += 1
        if (count >= 50) {
          return messages.toList
        }
      }
      return messages.toList
    }

    def createMessageScanRow(roomId: Long): Array[Byte] = {
      val values = Array[AnyRef] (
        //  hash.hash(Bytes.toBytes(roomId)).asInstanceOf[AnyRef],
      hash.hash(Bytes.toBytes(roomId)).toLong.asInstanceOf[AnyRef],
      roomId.asInstanceOf[AnyRef]
    )
      val positionedByteRange = 
        new SimplePositionedByteRange(messageRowSchema.encodedLength(values))
      messageRowSchema.encode(positionedByteRange, values)
      positionedByteRange.getBytes()
    }

    def convertToMessage(result: Result): Message  = {
      Message(
        Bytes.toString(result.getValue(Bytes.toBytes("m"), Bytes.toBytes("messageId"))),
        Bytes.toLong(result.getValue(Bytes.toBytes("m"), Bytes.toBytes("userId"))),
        "username", //TODO
        Bytes.toString(result.getValue(Bytes.toBytes("m"), Bytes.toBytes("body"))),
        Long.MaxValue - messageRowSchema.decode(new SimplePositionedByteRange(result.getRow()), 1).asInstanceOf[Long]
      )
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
        if (increase) {
          return bytes
        }
      }
      return bytes
    }
}

case class Message (
  messageId: String,
  userId: Long,
  userName: String,
  body: String,
  postAt: Long
)
