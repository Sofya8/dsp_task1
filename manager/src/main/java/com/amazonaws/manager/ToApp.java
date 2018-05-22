package com.amazonaws.manager;


import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class ToApp implements Runnable {
	private FromApp fromApp;
	private final String WtoM;
	private final String MtoA;
	private final AmazonSQS mysqs;
	private final AmazonS3 s3;
	private String bucketName=null;
	private String fileName=null;
	private String localAppName=null;
	private String outputFileName=null;
	private int numOfReviews=0;


	public ToApp(FromApp fromApp, String wtoM, String mtoA, AmazonSQS mysqs, AmazonS3 s3) {
		this.fromApp = fromApp;
		WtoM = wtoM;
		MtoA = mtoA;
		this.mysqs = mysqs;
		this.s3 = s3;
	}

	
	public void run() {
		//we don't need this check any more
		if(fromApp.initialized && fromApp.numOfReviews==0) {
			return;
		}
			
		String output="";
		String messageReceiptHandle;
		ReceiveMessageRequest receiveMessageRequest;
		List<Message> messages;
		Map<String,MessageAttributeValue> attributes=new HashMap<String,MessageAttributeValue>();
		while(!fromApp.initialized) {
			try {
				Thread.currentThread().sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		bucketName=fromApp.bucketName;
		fileName=fromApp.fileName;
		localAppName=fromApp.localAppName;
		numOfReviews=fromApp.numOfReviews;
		outputFileName=fromApp.outputFileName;
		
		
		
		
		while(numOfReviews>0) {
			receiveMessageRequest = new ReceiveMessageRequest(WtoM);
			messages = mysqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();
			if(!messages.isEmpty()) {
				for(Message message:messages) {
					if(message.getMessageAttributes().get("FileName").getStringValue().equals(fileName)) {
						output=output+message.getBody()+'\n';
						numOfReviews--;
						messageReceiptHandle = message.getReceiptHandle();
						mysqs.deleteMessage(new DeleteMessageRequest()
								.withQueueUrl(WtoM)
								.withReceiptHandle(messageReceiptHandle));
					}
				}
			}
		}
		fileName="sol"+fileName;
		ObjectMetadata metadata =new ObjectMetadata();
		InputStream inputStream = new ByteArrayInputStream((output.substring(0, output.length()-1)).getBytes());
		PutObjectRequest req = new PutObjectRequest(bucketName, fileName, inputStream,metadata);
		s3.putObject(req);
		attributes.put("BucketName", new MessageAttributeValue().withDataType("String").withStringValue(bucketName));
		attributes.put("FileName", new MessageAttributeValue().withDataType("String").withStringValue(fileName));
		attributes.put("LocalAppName", new MessageAttributeValue().withDataType("String").withStringValue(localAppName));
		attributes.put("OutputFileName", new MessageAttributeValue().withDataType("String").withStringValue(outputFileName));
		mysqs.sendMessage(new SendMessageRequest(MtoA,"done").withMessageAttributes(attributes));
		
	}



}
