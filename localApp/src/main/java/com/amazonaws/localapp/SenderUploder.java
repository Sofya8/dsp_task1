package com.amazonaws.localapp;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class SenderUploder implements Runnable {

	private AWSCredentialsProvider credentialsProvider;
	private AmazonS3 s3;
	private AmazonSQS mysqs;
	private String[] inputsOutputs;
	private String AtoM;
	private String terminationqueue;
	private final String appId;
	private boolean terminate;
	private int counterOfFilesWhichSended=0;
	public volatile boolean dontSentToTheManager= false;
	
	public SenderUploder(AWSCredentialsProvider credentialsProvider, AmazonS3 s3, AmazonSQS mysqs,
			String[] inputs,String appId,boolean terminate) {
		this.credentialsProvider = credentialsProvider;
		this.s3 = s3;
		this.mysqs = mysqs;
		this.inputsOutputs = inputs;
		this.appId=appId;
		this.terminate=terminate;
		try {
			 // searches for existing queues
			 for (String queueUrl : mysqs.listQueues().getQueueUrls()) {
				 	if(queueUrl.contains("AtoM"))
				 		AtoM=queueUrl;
				 	else if(queueUrl.contains("terminationqueue"))
				 		terminationqueue=queueUrl;
	            }
		}catch (AmazonServiceException ase) {
		   System.out.println("SenderUploder-ERROR-in constractor");
           System.out.println("Caught an AmazonServiceException, which means your request made it " +
                   "to Amazon SQS, but was rejected with an error response for some reason.");
           System.out.println("Error Message:    " + ase.getMessage());
           System.out.println("HTTP Status Code: " + ase.getStatusCode());
           System.out.println("AWS Error Code:   " + ase.getErrorCode());
           System.out.println("Error Type:       " + ase.getErrorType());
           System.out.println("Request ID:       " + ase.getRequestId());
       } catch (AmazonClientException ace) {
           System.out.println("Caught an AmazonClientException, which means the client encountered " +
                   "a serious internal problem while trying to communicate with SQS, such as not " +
                   "being able to access the network.");
           System.out.println("Error Message: " + ace.getMessage());
       }
	}

	
	public void run() {
		String key;
		Map<String,MessageAttributeValue> attributes=new HashMap<String,MessageAttributeValue>();
		attributes.put("LocalAppName", new MessageAttributeValue().withDataType("String").withStringValue(appId));
		
		
		int numOfFiles=inputsOutputs.length/2;
		
		String bucketName = (credentialsProvider.getCredentials()
				.getAWSAccessKeyId() + "0" + appId.replace('\\', '0').
				replace('/','0').replace(':', '0').replace(' ', '0')).toLowerCase();
	
		
		
		attributes.put("BucketName", new MessageAttributeValue().withDataType("String").withStringValue(bucketName));
		s3.createBucket(bucketName);
		for(int i=0;i<numOfFiles;i++) {
			if(terminated())
				break;
			//how many files will actually be in progress
			counterOfFilesWhichSended = counterOfFilesWhichSended+1;
			key=null;
			try {
				    String directoryName="C:\\Users\\micha\\Desktop\\input\\";
				 	File dir = new File(directoryName);
				 	for (File file : dir.listFiles()) {					 
				 		if(file.getName().contains(inputsOutputs[i])) {
				 			System.out.println("The file "+inputsOutputs[i]+" sent to processing");
				 			key = inputsOutputs[i].replace('\\', '0').replace('/','0').replace(':', '0');
					 		PutObjectRequest req = new PutObjectRequest(bucketName , key , file);
							s3.putObject(req);
					 	}	
				 	}
				 	
					attributes.put("FileName", new MessageAttributeValue().withDataType("String").withStringValue(key));
					attributes.put("OutputFileName", new MessageAttributeValue().withDataType("String")
							.withStringValue(inputsOutputs[i+numOfFiles]));
					sendMessage(AtoM,"new file",attributes);
					attributes.remove("FileName");
					attributes.remove("OutputFileName");
             
			}catch (AmazonServiceException ase) {
				System.out.println("Caught an AmazonServiceException, which means your request made it "
						+ "to Amazon S3, but was rejected with an error response for some reason.");
				System.out.println("Error Message:    " + ase.getMessage());
				System.out.println("HTTP Status Code: " + ase.getStatusCode());
				System.out.println("AWS Error Code:   " + ase.getErrorCode());
				System.out.println("Error Type:       " + ase.getErrorType());
				System.out.println("Request ID:       " + ase.getRequestId());
			} catch (AmazonClientException ace) {
				System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
				System.out.println("Error Message: " + ace.getMessage());
			}
		}
		if(counterOfFilesWhichSended==0) {
			dontSentToTheManager=true;
		}
		
		
		if(terminate && !terminated()) { 
			sendMessage(terminationqueue,"terminate",attributes);
		}
		Thread.currentThread().interrupt();
	}
	
	
	/**
	 * sends a message to the manager via the AtoM queue with the following format:
	 * "appId
	 * bucketName
	 * key"
	 * 
	 * @param mes
	 * @param attributes 
	 */
	private void sendMessage(String url,String mes, Map<String, MessageAttributeValue> attributes) {
		try {
			SendMessageRequest smr= new SendMessageRequest(url ,mes).withMessageAttributes(attributes);
			mysqs.sendMessage(smr);

		}catch(AmazonServiceException ase) {
	           System.out.println("Caught an AmazonServiceException, which means your request made it " +
	                   "to Amazon SQS, but was rejected with an error response for some reason.");
	           System.out.println("Error Message:    " + ase.getMessage());
	           System.out.println("HTTP Status Code: " + ase.getStatusCode());
	           System.out.println("AWS Error Code:   " + ase.getErrorCode());
	           System.out.println("Error Type:       " + ase.getErrorType());
	           System.out.println("Request ID:       " + ase.getRequestId());
	       } catch (AmazonClientException ace) {
	           System.out.println("Caught an AmazonClientException, which means the client encountered " +
	                   "a serious internal problem while trying to communicate with SQS, such as not " +
	                   "being able to access the network.");
	           System.out.println("Error Message: " + ace.getMessage());
	       }
		
	}

	/**
	 * checks for message from the termination queue - if there is one and it contains this localapp ID
	 * then it means, that the manager was terminated by other localapp who's id
	 * will not be in this message
	 * @return
	 */
	private boolean terminated() {
		 ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(terminationqueue);
         List<Message> messages = mysqs.receiveMessage(receiveMessageRequest).getMessages();
	     if(!messages.isEmpty()) {    
         	for(Message message : messages) {
	        	 mysqs.changeMessageVisibility(terminationqueue,message.getReceiptHandle(),0);
	        	 if(message.getBody().contains("the manager got terminat")||message.getBody().contains(appId))
	        		 return true;
	         }
	     }
		return false;
	}

	
}
