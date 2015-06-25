package bench.drivers

import bench._
import akka.actor._

import com.typesafe.config._

import java.util.List;
import java.util.Map.Entry;
import java.util.Date
import scala.util.Random

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

class AmazonSQSPutDriver(operation: String, stats: ActorRef, config: Config) extends
  Driver(operation, stats, config) {

   var sqs: AmazonSQS = _
  //  var sqs: AmazonSQSAsyncClient = _
  var queueUrl: String = _
  val msgLen = config.getInt("message_length")

  override def setup(): Boolean = {
    val credentials = new ProfileCredentialsProvider().getCredentials();
    this.sqs = new AmazonSQSClient(credentials)
    //  this.sqs = new AmazonSQSAsyncClient(credentials)
    this.sqs.setRegion(Region.getRegion(Regions.AP_NORTHEAST_1))
    this.queueUrl = sqs.createQueue(new CreateQueueRequest("MyQueue")).getQueueUrl();
    true
  }

  override def run(): (Boolean, Long, Long) = {

    val start = System.currentTimeMillis

    try {
      val msg = start + Random.alphanumeric.take(msgLen).mkString
      this.sqs.sendMessage(new SendMessageRequest(this.queueUrl, msg))
      // this.sqs.sendAsyncMessage(new SendMessageRequest(this.queueUrl, "This is my message text."))
      val endAt = System.currentTimeMillis
      val elapsedMillis= endAt - start
      (true, endAt, elapsedMillis)
    } catch {
       case e: java.net.SocketTimeoutException => {
         log.error("Socket Timeout Exception has occured. reconecting...")
         setup()
         val endAt = System.currentTimeMillis
         val elapsedMillis= endAt - start
         (false, endAt, elapsedMillis)
       }
       case e: Throwable => {
         log.error("" + e)
         val endAt = System.currentTimeMillis
         val elapsedMillis= endAt - start
         (false, endAt, elapsedMillis)
       }
     }
  }
}

