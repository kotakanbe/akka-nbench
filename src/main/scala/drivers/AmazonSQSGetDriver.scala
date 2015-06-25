package bench.drivers

import bench._
import akka.actor._
import collection.JavaConversions._

import com.typesafe.config._

import java.util.List;
import java.util.Map.Entry;
import java.util.Date

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

class AmazonSQSGetDriver(operation: String, stats: ActorRef, config: Config) extends Driver(operation, stats, config) {

  var sqs: AmazonSQS = _
  //  var sqs: AmazonSQSAsyncClient = _
  var queueUrl: String = _

  override val getOperation = () => {
    run _
  }

  override def setup(): Boolean = {
    val credentials = new ProfileCredentialsProvider().getCredentials();
    this.sqs = new AmazonSQSClient(credentials)
    //  this.sqs = new AmazonSQSAsyncClient(credentials)
    this.sqs.setRegion(Region.getRegion(Regions.AP_NORTHEAST_1))
//TODO queuename
    this.queueUrl = sqs.createQueue(new CreateQueueRequest("MyQueue")).getQueueUrl();
    true
  }

  override def run(): (Boolean, Long, Long) = {
    val start = System.currentTimeMillis
    try {

      //  this.sqs.sendMessage(new SendMessageRequest(this.queueUrl, "This is my message text."))
      val request: ReceiveMessageRequest = new ReceiveMessageRequest(this.queueUrl)
      request.setVisibilityTimeout(5)

      // TODO enable to set LongPolling on off in config file
      // http://docs.aws.amazon.com/ja_jp/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html
      // http://qiita.com/uzresk/items/7a2a0d7ef85445c50e31
      // Long Polling
      //  request.setWaitTimeSeconds(20)
      // Short Polling
      request.setWaitTimeSeconds(0)

      // TODO enable to set maxNumberOfMessages on off in config file
      // http://docs.aws.amazon.com/ja_jp/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html
      // http://qiita.com/uzresk/items/7a2a0d7ef85445c50e31
      // https://github.com/amazonwebservices/aws-sdk-for-java/blob/master/src/main/java/com/amazonaws/services/sqs/model/ReceiveMessageRequest.java#L208
      //  request.withMaxNumberOfMessages(10)
      request.withMaxNumberOfMessages(1)

      val messages: List[Message] = this.sqs.receiveMessage(request).getMessages()
      for (message <- messages) {
        //  System.out.println(message.getMessageId() + ":" + message.getBody())
        val deleteRequest = new DeleteMessageRequest()
        deleteRequest.setQueueUrl(this.queueUrl)
        deleteRequest.setReceiptHandle(message.getReceiptHandle())
        sqs.deleteMessage(deleteRequest)
      }

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

