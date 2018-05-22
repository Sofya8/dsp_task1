package com.amazonaws.manager;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.codec.binary.Base64;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class Manager {

	public static void main(String[] args) {
		 AWSCredentials myCredentials= new EnvironmentVariableCredentialsProvider().getCredentials();
		AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(myCredentials);
		AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
		AmazonSQS mysqs = AmazonSQSClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
		final AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
	
		
		final int numOfJobsPerWorker=Integer.valueOf(args[0]);
		int numOfJobs=0;
		String AtoM=find("AtoM",mysqs);
		String MtoA=find("MtoA",mysqs);
		String WtoM=find("WtoM",mysqs);
		String MtoW=find("MtoW",mysqs);
		String terminationQueue=find("terminationqueue",mysqs);
		ConcurrentLinkedQueue<String> workerIds= new ConcurrentLinkedQueue<String>();
		workerIds.addAll(runNewWorker(1,ec2,credentialsProvider));
		ConcurrentHashMap<String,Integer> apps=new ConcurrentHashMap<String,Integer>();
		String localApp;
		ExecutorService tPool = Executors.newFixedThreadPool(5);
		int howMuchMessagesTheManagerSend=0;
		while((localApp=gotTerminationMessage(mysqs,terminationQueue))==null) {
			howMuchMessagesTheManagerSend=howMuchMessagesTheManagerSend + handlingAmessage( ec2, mysqs, s3 , AtoM, MtoW, WtoM, MtoA, apps, numOfJobsPerWorker, credentialsProvider, workerIds,tPool);
		}
		//Prevent anther entries to "AtoM" queue
		preventEntryToAtoMQueue(mysqs,terminationQueue);
		//Finishing the rest of the messages
		while(!AtoMempty(mysqs,AtoM)) {			
			howMuchMessagesTheManagerSend=howMuchMessagesTheManagerSend +handlingAmessage( ec2, mysqs, s3 , AtoM, MtoW, WtoM, MtoA, apps, numOfJobsPerWorker, credentialsProvider, workerIds,tPool);
		}
		tPool.shutdown();
		
		//Whit until the tPool.shutdown() will complete
		while(!tPool.isTerminated()) {
			try {
				Thread.currentThread().sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		
		
		/////////
		//for statistics 
		statistics(mysqs,s3,WtoM,MtoW,workerIds,howMuchMessagesTheManagerSend);
		////////
		//Terminate all Workers
		terminateWorkers(ec2,workerIds);
		
		//When we get here all the messages are resolved 
		sendTerminationMessage(localApp,mysqs,terminationQueue,apps);
		
		
		collectDeadMessages(mysqs, apps, terminationQueue);
		
		sendKILLMEmessage( mysqs, terminationQueue);
	}
	
	private static void sendKILLMEmessage(AmazonSQS mysqs,String terminationqueue) {
		String Body="kill me";
		mysqs.sendMessage(new SendMessageRequest(terminationqueue,Body));
	}
//	private static void deleteTerminateMessage(AmazonSQS mysqs,String terminationqueue) {
//		 ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(terminationqueue);
//        List<Message> messages = mysqs.receiveMessage(receiveMessageRequest).getMessages();
//	     if(!messages.isEmpty()) {    
//        	for(Message message : messages) {	        	 
//	        	 if(!message.getBody().contains("the manager got terminat")) {
//	        		 mysqs.changeMessageVisibility(terminationqueue,message.getReceiptHandle(),0);
//	        	 }else {
//	        		 String messageReceiptHandle = message.getReceiptHandle();
//	 			     mysqs.deleteMessage(new DeleteMessageRequest()
//	 					.withQueueUrl(terminationqueue)
//	 					.withReceiptHandle(messageReceiptHandle));
//	        	 }
//	         }
//	     }
//	}

	
	private static int handlingAmessage(AmazonEC2 ec2,AmazonSQS mysqs,AmazonS3 s3 ,String AtoM,String MtoW,String WtoM,String MtoA,ConcurrentHashMap<String,Integer> apps,
			int numOfJobsPerWorker,AWSStaticCredentialsProvider credentialsProvider,ConcurrentLinkedQueue<String> workerIds,ExecutorService tPool) {
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(AtoM);
		List<Message> messages = mysqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();
		if(!messages.isEmpty()) {
				MessageProcessor messageProcessor=new MessageProcessor( ec2, mysqs, s3 , AtoM, MtoW, WtoM, MtoA, apps,
															numOfJobsPerWorker, credentialsProvider, workerIds,messages.get(0));
				tPool.execute(messageProcessor);
				return 1;
		}
		return 0;
	}
	
	private static void collectDeadMessages(AmazonSQS mysqs,ConcurrentHashMap<String,Integer> apps,String terminationQueue) {
		int numOfLocalAppLeftAlive=apps.size();
		while(numOfLocalAppLeftAlive>1) {
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(terminationQueue);
			List<Message> messages = mysqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();			
			if(!messages.isEmpty()) {
				for(Message message:messages) {
					if(message.getBody().contains("dead")) {
						numOfLocalAppLeftAlive--;
						String messageReceiptHandle = message.getReceiptHandle();
						mysqs.deleteMessage(new DeleteMessageRequest()
								.withQueueUrl(terminationQueue)
								.withReceiptHandle(messageReceiptHandle));
					}
				}
			}
		}
	}
	
	private static void statistics(AmazonSQS mysqs,AmazonS3 s3 , String WtoM,String MtoW,ConcurrentLinkedQueue<String> workerIds, int howMuchMessagesTheManagerSend) {
		String[] a =new String[1];//Because the .toArray(a) function
		String[] workers=workerIds.toArray(a);
		for (int i = 0; i < workers.length; i++) {
			Map<String,MessageAttributeValue> attributes = new HashMap<String,MessageAttributeValue>();
			attributes.put("WorkerId", new MessageAttributeValue().withDataType("String").withStringValue(workers[i]));
			String Body="Prepare to die!";
			mysqs.sendMessage(new SendMessageRequest(MtoW,Body).withMessageAttributes(attributes));
		}
		String output="The Manager send "+ howMuchMessagesTheManagerSend+" messages"+"\n" ;
		
		int numOfWorkersWeNeedToWhitForThem=workers.length;
		while(numOfWorkersWeNeedToWhitForThem>0) {
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(WtoM);
			List<Message> messages = mysqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();			
			if(!messages.isEmpty()) {
				for(Message message:messages) {
					if(message.getBody().contains("Worker")) {
						output=output+message.getBody()+"\n" ;		
						numOfWorkersWeNeedToWhitForThem--;
						String messageReceiptHandle = message.getReceiptHandle();
						mysqs.deleteMessage(new DeleteMessageRequest()
								.withQueueUrl(WtoM)
								.withReceiptHandle(messageReceiptHandle));
					}
				}
			}
		}
		String fileName="statistics";
		String bucketName="michaelandsofibucket";
		ObjectMetadata metadata =new ObjectMetadata();
		InputStream inputStream = new ByteArrayInputStream((output.substring(0, output.length()-1)).getBytes());
		PutObjectRequest req = new PutObjectRequest(bucketName, fileName, inputStream,metadata);
		s3.putObject(req);
	}

	
	private static boolean AtoMempty(AmazonSQS mysqs, String AtoM) {

		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(AtoM);
		List<Message> messages = mysqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();
		if(!messages.isEmpty()) {	
			try {
			  mysqs.changeMessageVisibility(AtoM, messages.get(0).getReceiptHandle(), 0);
			}catch (Exception e) {
				System.out.println("I STOP IN THE FANCTION AtoMempty WHEN messages.isEmpty()= "+ messages.isEmpty()
				+" !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			}
		}
		return messages.isEmpty();
	}
	

	private static void terminateWorkers(AmazonEC2 ec2, ConcurrentLinkedQueue<String> workerId) {
		if(!workerId.isEmpty()) {
			TerminateInstancesRequest terminate_request = new TerminateInstancesRequest();
			terminate_request.setInstanceIds(workerId);
			ec2.terminateInstances(terminate_request);
		}
	}


	private static void sendTerminationMessage(String localApp, AmazonSQS mysqs, String terminationQueue, Map<String, Integer> apps) {
		Map<String,MessageAttributeValue> attributes= new HashMap<String,MessageAttributeValue>();
		String mesBody="termination message";
		Set<Entry<String, Integer>> set=apps.entrySet();
		if(!set.isEmpty()){	
			 for(Entry<String, Integer> entry:set) {
					if((entry!= null) && (!entry.getKey().equals(localApp))) {
						mesBody=mesBody+'\n'+entry.getKey();
						attributes.put(entry.getKey(), new MessageAttributeValue().withDataType("String")
								.withStringValue(String.valueOf(entry.getValue())));
					}
						
			 }
	    }
		mysqs.sendMessage(new SendMessageRequest(terminationQueue,mesBody)
				.withMessageAttributes(attributes));
	}


	private static void preventEntryToAtoMQueue(AmazonSQS mysqs, String terminationQueue) {
		String Body="the manager got terminat";
		mysqs.sendMessage(new SendMessageRequest(terminationQueue,Body));
		
	}
	
	
	private static String gotTerminationMessage(AmazonSQS mysqs, String terminationQueue) {
		String ans=null;
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(terminationQueue);
		List<Message> messages = mysqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();
		if(messages.isEmpty())
			ans=null;
		else {
			ans=messages.get(0).getMessageAttributes().get("LocalAppName").getStringValue();
			String messageReceiptHandle = messages.get(0).getReceiptHandle();
			mysqs.deleteMessage(new DeleteMessageRequest()
					.withQueueUrl(terminationQueue)
					.withReceiptHandle(messageReceiptHandle));
		}
		return ans;
	}


	private static List<String> runNewWorker(int numWorkersOpen, AmazonEC2 ec2, AWSCredentialsProvider credentialsProvider) {
		int running=workersRunning(ec2);
		numWorkersOpen=numWorkersOpen-running;
		List<String> ids=new ArrayList<String>();
		if(numWorkersOpen>0){
			 RunInstancesRequest request = new RunInstancesRequest()
		        	    .withImageId("ami-6057e21a")
		        	    .withMinCount(numWorkersOpen)
		        	    .withMaxCount(numWorkersOpen)
		        	    .withInstanceType("t2.medium")
		        	    .withKeyName("keyPair")
		                .withSecurityGroups("sg")
		                .withUserData(getuserDataWorker(credentialsProvider));
			List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
			for(int i=0;i<instances.size();i++){
				CreateTagsRequest ctr = new CreateTagsRequest().withResources(instances.get(i).getInstanceId()).withTags(new Tag("Work","Worker"));
				ec2.createTags(ctr);
			}
			for(Instance instance:instances) {
				ids.add(instance.getInstanceId());
			}
		}
		return ids;
		
	}

	
	private static String getuserDataWorker(AWSCredentialsProvider credentialsProvider) {
		String userData = "";
		   
	    userData = userData + "#!/bin/bash"+"\n";
	    userData = userData +"exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1"+"\n";
	    userData = userData + "set -e -x"+"\n";
	    userData = userData + "sudo su"+"\n";
	    userData = userData + "sudo yum install -y java-1.8.0"+"\n";
	    userData = userData + "sudo yum remove -y java-1.7.0-openjdk"+"\n";
	    userData = userData + "AWS_ACCESS_KEY_ID="+ credentialsProvider.getCredentials().getAWSAccessKeyId() +"\n";
		userData = userData + "AWS_SECRET_ACCESS_KEY="+ credentialsProvider.getCredentials().getAWSSecretKey() +"\n";
		userData = userData + "AWS_DEFAULT_REGION=us-east-1"+"\n";
		userData = userData + "export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION"+"\n"; 
	    userData = userData + "aws s3 cp s3://michaelandsofibucket/worker.jar worker.jar"+"\n";
	    userData = userData + "java -jar worker.jar"+"\n";
	   

		String base64UserData = null;
	    try {
	        base64UserData = new String( Base64.encodeBase64( userData.getBytes( "UTF-8" )), "UTF-8" );
	    } catch (UnsupportedEncodingException e) {
	        e.printStackTrace();
	    }
	    return base64UserData;
		
	}


	private static int workersRunning(AmazonEC2 ec2) {
		DescribeInstancesRequest request = new DescribeInstancesRequest();

		List<String> valuesJob = new ArrayList<String>();
		valuesJob.add("Worker");
		Filter filter1 = new Filter("tag:Work", valuesJob);

		List<String> statusValues = new ArrayList<String>();
		statusValues.add("running");
		statusValues.add("pending");

		Filter statusTag = new Filter("instance-state-name", statusValues);


		DescribeInstancesResult result = ec2.describeInstances(request.withFilters(filter1,statusTag));
		List<Reservation> reservations = result.getReservations();
		int aliveWorkers = 0;
		for(Reservation res: reservations){
			aliveWorkers += res.getInstances().size();
		}

		return aliveWorkers;
	}


	/**
	 * finds the queue url that contains @param "string" as a substring
	 * @param string
	 * @param mysqs
	 * @return
	 */
	private static String find(String string, AmazonSQS mysqs) {
		try {
			 for (String queueUrl : mysqs.listQueues().getQueueUrls()) {
				 	if(queueUrl.contains(string)) {
				 		return queueUrl;
				 	}
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
		return null;
	}

	
}
