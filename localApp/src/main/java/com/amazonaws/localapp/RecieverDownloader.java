package com.amazonaws.localapp;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class RecieverDownloader implements Runnable {
	private AmazonS3 s3;
	private AmazonSQS mysqs;
	private String MtoA;
	private ArrayList<String> buckets=new ArrayList<String>();
	private String appId;
	private int doneFiles=0;
	private int remainFiles=0;
	private String terminationqueue;
	
	public RecieverDownloader(AmazonSQS mysqs, AmazonS3 s3,
			int remainFiles,String appId) {
		this.mysqs = mysqs;
		this.s3 = s3;
		this.appId=appId;
		this.remainFiles=remainFiles;
		try {
			 for (String queueUrl : mysqs.listQueues().getQueueUrls()) {
				 	if(queueUrl.contains("MtoA"))
				 		MtoA=queueUrl;
				 	else if(queueUrl.contains("terminationqueue"))
				 		terminationqueue=queueUrl;
			 }
		}catch (AmazonServiceException ase) {
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
		
		String bucketName;
		String key;
		List<Message> adressOfOutputFile;
		while((remainFiles-doneFiles)>0) {
			adressOfOutputFile=getAddressFromQueue();
			if(!adressOfOutputFile.isEmpty()) {
				for(Message message:adressOfOutputFile) {
					bucketName=message.getMessageAttributes().get("BucketName").getStringValue();
					key=message.getMessageAttributes().get("FileName").getStringValue();
					downloadFileAndCreateHtml(bucketName,key,message.getMessageAttributes().get("OutputFileName").getStringValue());
					if(!buckets.contains(bucketName)) {
						buckets.add(bucketName);
					}
					checkTerminationQueue();
					doneFiles++;
				}
			}
		}
		deleteBuckets();
		//shuts this thread down
		Thread.currentThread().interrupt();
	}
	
	

	
	private void checkTerminationQueue() {
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(terminationqueue);
	    List<Message> messages = mysqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();
	    if(!messages.isEmpty()) {
			mysqs.changeMessageVisibility(terminationqueue,messages.get(0).getReceiptHandle(),0);
		    for(Message message : messages) {
			   	if(message.getBody().contains(appId)) {
			   		remainFiles=Integer.valueOf(message.getMessageAttributes().get("LocalAppName").getStringValue());
			   	}
		    }
	    }   
	}


	/**
	 * 
	 * deletes the buckets of this local app from the s3
	 */
	private void deleteBuckets() {
		try {
			for(String bucket:buckets) {
				s3.deleteBucket(bucket);
			}
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

	
	/**
	 * downloads an s3 object and then deletes it
	 * @param bucketName
	 * @param key
	 */
	private void downloadFileAndCreateHtml(String bucketName, String key,String outputFileName) {
		try {
			S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
			createHtmlFile(object,outputFileName);	
			s3.deleteObject(bucketName, key);
			System.out.println("The "+outputFileName+".html is ready!");
		
		}catch (AmazonServiceException ase) {
			System.out.println("downloadFileAndCreateHtml");
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

	/**
	 * we have here as an input an s3object. 
	 * Actually it's an inputstream and the name of the output file.
	 *
	 * @param object
	 * @param outputFileName 
	 */
	private void createHtmlFile(S3Object object, String outputFileName) {
		 
	     try {
	    	 	String outPutName= outputFileName+".html";
	    	 	BufferedReader txtfile = new BufferedReader(new InputStreamReader(object.getObjectContent()));
	            OutputStream htmlfile= new FileOutputStream(new File("C:\\Users\\micha\\Desktop\\input\\"+outPutName));
	            PrintStream printhtml = new PrintStream(htmlfile);
	            
	            Gson gson = new GsonBuilder().disableHtmlEscaping().create();
	            ArrayList<String> txtbyLine=new ArrayList<String>();	     
	            String temp = "";
	            String txtfiledata = "";
	            
	            String htmlheader="<html>";
	            htmlheader+="<body>";
	            String htmlfooter="</body></html>";
	            int linenum = 0 ;

	            while ((txtfiledata = txtfile.readLine())!= null){     
	            	
	            	txtfiledata=txtfiledata.substring(txtfiledata.indexOf("{"));
	            	DspSolution sol = gson.fromJson(txtfiledata,DspSolution.class);
	            	txtbyLine.add(toStringHtml(sol));
	            	linenum=linenum+1;
	            } 
	            
	            for(int i=0;i<linenum;i++){
	                if(i == 0){
	                    temp = htmlheader + txtbyLine.get(0);
	                    txtbyLine.set(0,temp);
	                }
	                if(linenum == i+1){
	                    temp = txtbyLine.get(i) + htmlfooter;
	                    txtbyLine.set(i,temp);
	                }
	                printhtml.println(txtbyLine.get(i));
	            }
	        printhtml.close();
	        htmlfile.close();
	        txtfile.close();
	    }
	    catch (Exception e) {}	
		 	
	}
	
	
	
	
	public String toStringHtml(DspSolution sol) {
		String toAdd="<p><strong>Review:</strong><span style="
					+"\"color:"
					+ sol.getColor() 
					+ "\"> "
					+ sol.getReview() 
					+"</span></p>"
					+"<p><strong>File link:</strong> "
					+"<a href=\""
					+sol.getLink()
					+"\" target=\"_blank\">"
					+sol.getLink()
					+"</a>"
					+" </p>"
					+"<p><strong>Sentiment result:</strong> "
					+sol.getSentiment()
					+" , <strong>Entities:</strong> "
					+sol.getEntities()
					+" , <strong>Rating:</strong>"
					+ sol.getRating()
					+" , <strong>Sarcastic:</strong> "
					+sol.getSarcastic()
					+" </p><br><hr><br>";
		return toAdd;
	}

	
	/**
	 * builds a list of addresses of the files this local application send to the manager
	 * and then immediately deletes the message
	 * 
	 * if it's a message of how many files the manager took care about, the function will 
	 * update the "remainFiles" field
	 * 
	 * @return list of addresses of files from the s3
	 */
	private List<Message> getAddressFromQueue() {
		List<Message> adresses=new ArrayList<Message>();
		String messageReceiptHandle;
		try {
			 ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(MtoA);
			 receiveMessageRequest.setMaxNumberOfMessages(5);
	         List<Message> messages = mysqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();
	         for(Message message: messages) {
	        	 if(message.getMessageAttributes().get("LocalAppName").getStringValue().equals(appId)) {
	         		adresses.add(message);
	         		messageReceiptHandle = message.getReceiptHandle();
					mysqs.deleteMessage(new DeleteMessageRequest()
							.withQueueUrl(MtoA)
							.withReceiptHandle(messageReceiptHandle));
	         	}
	
	         }
			
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
		return adresses;
	}
}
