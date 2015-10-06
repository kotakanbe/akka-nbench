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

class HBaseMessagingServiceDriver(operation: String, stats: ActorRef, config: Config) extends Driver(operation, stats, config) {

  override val getOperation = () => {
    operation match {
      case "sender" => send _
      //  case "initial" => initial _
      case "reader" => attendAndRead _
    }
  }

  var conn: Connection = _
  var table: Table = _
  val nsName = "ns"
  val tableName= s"${nsName}:message"
  val colFamily = "m"

  var hash = Hash.getInstance(Hash.MURMUR_HASH3)

  var latestMessage: Message = null

  var messageRowSchema = 
    new StructBuilder().
    add(new RawLong()). // messageID
    add(new RawLong()). //
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
      Thread.sleep(100)
      val start = System.currentTimeMillis
      try {
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
      var postAt = System.currentTimeMillis()
      log.info("sendMessage:\t"+postAt)
      val messageId = java.util.UUID.randomUUID().toString()
      val row = createMessageRow(roomId, postAt, messageId)
      val put = new Put(row)
      put.addColumn(Bytes.toBytes("m"), Bytes.toBytes("messageId"), Bytes.toBytes(messageId))
      put.addColumn(Bytes.toBytes("m"), Bytes.toBytes("userId"), Bytes.toBytes(userId))
      put.addColumn(Bytes.toBytes("m"), Bytes.toBytes("body"), Bytes.toBytes(body))
      this.table.put(put)
    }

    def initial(): (Boolean, Long, Long) = {
      Thread.sleep(500)
      val start = System.currentTimeMillis
      try {

        val msgs = getInitialMessages(1, List(0))
        log.info(s"num msgs ${msgs.length}")
        //  msgs.foreach {msg => log.info(""+msg) }

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
      val scanner: ResultScanner = this.table.getScanner(scan)

      val messages = scala.collection.mutable.ListBuffer.empty[Message]
      var count = 0
      scanner.iterator.foreach { result =>
        messages.append(convertToMessage(result))
        count += 1
        if (count >= 50) {
          return messages.toList
        }
      }
      return messages.toList
    }

    def attendAndRead(): (Boolean, Long, Long) = {
      val start = System.currentTimeMillis

      try {
        Option(this.latestMessage) match {
          case None => 
            val msgs = getInitialMessages(1, List(0))
            if( !msgs.isEmpty) this.latestMessage = msgs.head
            log.info("attended.") 
            //  log.info(""+msgs.head)
            //  msgs.foreach {m => log.info(""+m)}
          case _ => ;
        }

        Thread.sleep(2000)
        val newMsgs  = getNewMessages(1, this.latestMessage, List(0))
        //  log.info(s"received num: ${newMsgs.length}")
        newMsgs.foreach {m => log.info(s"received: ${m}")}
        if( !newMsgs.isEmpty) this.latestMessage = newMsgs.head

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

    def getNewMessages(roomId: Long, stopMessage: Message, blockUsers: List[Long]): List[Message] = {
      val startRow: Array[Byte] = createMessageScanRow(roomId)
      val stopRow: Array[Byte] = createMessageRow(roomId, stopMessage.postAt, stopMessage.messageId)

      val scan = new Scan(startRow, stopRow)
      val filterList = new FilterList(Operator.MUST_PASS_ALL)

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

      scanner.iterator.foreach { result =>
        val msg = convertToMessage(result)
        //  log.info(""+msg)
        messages.append(msg)
        count += 1
        if (count >= 50) {
          return messages.toList
        }
      }
      return messages.toList
    }

    def createMessageRow(roomId: Long, postAt: Long, messageId: String): Array[Byte] = {
      //  log.info("createMessageRow:\t"+(Long.MaxValue-postAt))
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


    def createMessageScanRow(roomId: Long): Array[Byte] = {
      val values = Array[AnyRef] (
        //  hash.hash(Bytes.toBytes(roomId)).asInstanceOf[AnyRef],
      hash.hash(Bytes.toBytes(roomId)).toLong.asInstanceOf[AnyRef],
      roomId.asInstanceOf[AnyRef]
    )
      val positionedByteRange = 
        new SimplePositionedMutableByteRange(messageRowSchema.encodedLength(values))
      messageRowSchema.encode(positionedByteRange, values)
      positionedByteRange.getBytes()
    }

    def convertToMessage(result: Result): Message  = {
      Message(
        Bytes.toString(result.getValue(Bytes.toBytes("m"), Bytes.toBytes("messageId"))),
        Bytes.toLong(result.getValue(Bytes.toBytes("m"), Bytes.toBytes("userId"))),
        "username", //TODO
        Bytes.toString(result.getValue(Bytes.toBytes("m"), Bytes.toBytes("body"))),
        Long.MaxValue - messageRowSchema.decode(new SimplePositionedMutableByteRange(result.getRow()), 2).asInstanceOf[Long]
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
        if (!increase) {
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
) {
  override def toString: String = {
    s"postAt:\t${postAt}, mid: ${messageId}, uid: ${userId}, body: ${body}"
  }
}
