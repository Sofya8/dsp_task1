package com.amazonaws.manager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class FromApp implements Runnable {
	
	private final String AtoM;
	private final String MtoW;
	private final AmazonSQS mysqs;
	private final AmazonS3 s3;
	public volatile String bucketName=null;
	public volatile String fileName=null;
	public volatile String localAppName=null;
	public volatile  String outputFileName=null;
	public volatile int numOfReviews=0;
	public volatile boolean initialized=false;
	public volatile Message message;
	
	
	
	public FromApp(String AtoM, String MtoW, AmazonSQS mysqs, AmazonS3 s3,Message message) {
		super();
		this.AtoM = AtoM;
		this.MtoW = MtoW;
		this.mysqs = mysqs;
		this.s3 = s3;
		this.message = message;
	}

	
	public void run() {
		List<String>reviews;
		Map<String,MessageAttributeValue> attributes;
		bucketName=message.getMessageAttributes().get("BucketName").getStringValue();
		fileName=message.getMessageAttributes().get("FileName").getStringValue();
		localAppName=message.getMessageAttributes().get("LocalAppName").getStringValue();
		outputFileName=message.getMessageAttributes().get("OutputFileName").getStringValue();
		
		S3Object object = s3.getObject(new GetObjectRequest(bucketName, fileName));
		String messageReceiptHandle = message.getReceiptHandle();
		mysqs.deleteMessage(new DeleteMessageRequest()
				.withQueueUrl(AtoM)
				.withReceiptHandle(messageReceiptHandle));
		BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
		reviews=getReviewsForSending(reader);
		s3.deleteObject(bucketName, fileName);
		numOfReviews=reviews.size();
		attributes = new HashMap<String,MessageAttributeValue>();
		attributes.put("BucketName", new MessageAttributeValue().withDataType("String").withStringValue(bucketName));
		attributes.put("FileName", new MessageAttributeValue().withDataType("String").withStringValue(fileName));
		attributes.put("LocalAppName", new MessageAttributeValue().withDataType("String").withStringValue(localAppName));
		attributes.put("OutputFileName", new MessageAttributeValue().withDataType("String").withStringValue(outputFileName));
		for (String review : reviews) {
			mysqs.sendMessage(new SendMessageRequest(MtoW,review).withMessageAttributes(attributes));	
		}
		initialized=true;
		
	}

	

	private List<String> getReviewsForSending(BufferedReader reader){
		List<String> ans=new ArrayList<String>();
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		try {
			String sCurrentLine;
			while ((sCurrentLine = reader.readLine()) != null) {
				sCurrentLine=sCurrentLine.substring(sCurrentLine.indexOf("{"));
				DspInputFile input = gson.fromJson(sCurrentLine,DspInputFile.class);
				for(DspReview rev:input.getReviews()) {
					String jsonString = gson.toJson(rev);
					ans.add(jsonString);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return ans;
	}
}
