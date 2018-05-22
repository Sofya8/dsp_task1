package com.amazonaws.localapp;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.commons.codec.binary.Base64;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;


public class localApp {

	public static void main(String[] args) {
		final String appId=UUID.randomUUID().toString();
		AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
		
		AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
	                .withCredentials(credentialsProvider)
	                .withRegion("us-east-1")
	                .build();
		AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
		AmazonSQS mysqs = AmazonSQSClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
		int numInputFiles;
		boolean terminate=args[args.length-1].equals("terminate");
		
		String[] inputs;
			
		if(terminate)
			numInputFiles=args.length-2;
		else
			numInputFiles=args.length-1;
		
		int n=Integer.valueOf(args[numInputFiles]);
		
		initializeQueues(mysqs);
		

		
		String managerInstanceId;
		if((managerInstanceId=thereAreInstaces(ec2))==null)
			managerInstanceId=initialize(ec2,credentialsProvider,n);
		
		
		
		inputs=new String[numInputFiles];
		
		for (int i=0;i<numInputFiles;i++) {
			inputs[i]=args[i];
		}
		
		
		
		SenderUploder su=new SenderUploder(credentialsProvider,s3,mysqs,inputs,appId,terminate);
		Thread sender=new Thread(su);
		sender.start();
		try {
			sender.join();
		} catch (InterruptedException e) {
			System.out.println("in local app (the join)");
			e.printStackTrace();
		}
		//if the manager got termination message
		if(su.dontSentToTheManager) {
			System.out.println("The Manager already got termination message so this LocalApp does not evaluated.");
			return;
		}
		
		RecieverDownloader rd= new RecieverDownloader(mysqs, s3, numInputFiles/2,appId);		
		Thread reciever=new Thread(rd);
		reciever.start();
		try {
			reciever.join();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		terminateIfYouNeed(terminate,ec2,managerInstanceId,mysqs);
		
	}
	
	private static String initialize(AmazonEC2 ec2, AWSCredentialsProvider credentialsProvider, int n) {
		try {
			
	        RunInstancesRequest request = new RunInstancesRequest()
	        	    .withImageId("ami-6057e21a")
	        	    .withMinCount(1)
	        	    .withMaxCount(1)
	        	    .withInstanceType("t2.micro")
	        	    .withKeyName("keyPair")
	                .withSecurityGroups("sg")
	                .withUserData(startManager(ec2,credentialsProvider,n));
	        
	        List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
	        
	        	
	        
	    	for (Instance instance : instances) {
		    	  CreateTagsRequest createTagsRequest = new CreateTagsRequest();
		    	  createTagsRequest.withResources(instance.getInstanceId()).withTags(new Tag("Manager", "Active"));
		    	  ec2.createTags(createTagsRequest);
		    	  
	    	 }
	    	System.out.println("Manager created.");
	    	return instances.get(0).getInstanceId(); 
		} catch (AmazonServiceException ase) {
			System.out.println("initialize");
	        System.out.println("Caught Exception: " + ase.getMessage());
	        System.out.println("Reponse Status Code: " + ase.getStatusCode());
	        System.out.println("Error Code: " + ase.getErrorCode());
	        System.out.println("Request ID: " + ase.getRequestId());
	    }
		return null;
    }

	private static String startManager(AmazonEC2 ec2, AWSCredentialsProvider credentialsProvider, int n) {
		String userData = "";
	    userData = userData + "#!/bin/bash"+"\n";
	    userData = userData +"exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1"+"\n";
	    userData = userData + "set -e -x"+"\n";
	    userData = userData + "sudo su"+"\n";
	    userData = userData + "sudo yum install -y java-1.8.0"+"\n";
	    userData = userData + "sudo yum remove -y java-1.7.0-openjdk"+"\n";
	    userData = userData + "AWS_ACCESS_KEY_ID="+ credentialsProvider.getCredentials()
	    	.getAWSAccessKeyId()  +"\n";
		userData = userData + "AWS_SECRET_ACCESS_KEY="+ credentialsProvider
				.getCredentials().getAWSSecretKey() +"\n";
		userData = userData + "AWS_DEFAULT_REGION=us-east-1"+"\n";
		userData = userData + "export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION"+"\n";
	    userData = userData + "aws s3 cp s3://michaelandsofibucket/manager.jar manager.jar"+"\n";
	    userData = userData + "java -jar manager.jar "+ n +"\n";

	    String base64UserData = null;
	    try {
	        base64UserData = new String( Base64.encodeBase64( userData.getBytes( "UTF-8" )), "UTF-8" );
	    } catch (UnsupportedEncodingException e) {
	        e.printStackTrace();
	    }
	    return base64UserData;
	}
		

	
	private static String thereAreInstaces(AmazonEC2 ec2) {
		
		try {
			DescribeInstancesRequest request = new DescribeInstancesRequest();
		    DescribeInstancesResult response = ec2.describeInstances(request);
		 
		    for(Reservation reservation : response.getReservations()) {
		        for(Instance instance : reservation.getInstances()) {		       
		            for(Tag tag :instance.getTags()) {
			  	  		String state=instance.getState().getName();			  	  	
		            	if(tag.getKey().equals("Manager") && (state.equals("running") || state.equals("pending")) ){		            		
		            		return instance.getInstanceId();
			  	  		}
			  	  	}
		        }
		    }
			
		} catch (AmazonServiceException ase) {
			System.out.println("thereAreInstaces");
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
		}
		return null;
	}

	private static void initializeQueues(AmazonSQS mysqs) {
		if(mysqs.listQueues().getQueueUrls().size()<5){
			try {
				CreateQueueRequest createQueueRequest = new CreateQueueRequest("AtoM"+ UUID.randomUUID());
				createQueueRequest.setSdkClientExecutionTimeout(1000*120);
				mysqs.createQueue(createQueueRequest);
				createQueueRequest = new CreateQueueRequest("MtoW"+ UUID.randomUUID());
				createQueueRequest.setSdkClientExecutionTimeout(1000*60);
				mysqs.createQueue(createQueueRequest);
				createQueueRequest = new CreateQueueRequest("WtoM"+ UUID.randomUUID());
				createQueueRequest.setSdkClientExecutionTimeout(1000*60);
				mysqs.createQueue(createQueueRequest);
				createQueueRequest = new CreateQueueRequest("MtoA"+ UUID.randomUUID());
				createQueueRequest.setSdkClientExecutionTimeout(1000*60);
				mysqs.createQueue(createQueueRequest);
				createQueueRequest = new CreateQueueRequest("terminationqueue"+ UUID.randomUUID());
				createQueueRequest.setSdkClientExecutionTimeout(1000*60);
				mysqs.createQueue(createQueueRequest);
			}catch (AmazonServiceException ase) {
				System.out.println("initializeQueues-ASEexception");
	            System.out.println("Caught an AmazonServiceException, which means your request made it " +
	                    "to Amazon SQS, but was rejected with an error response for some reason.");
	            System.out.println("Error Message:    " + ase.getMessage());
	            System.out.println("HTTP Status Code: " + ase.getStatusCode());
	            System.out.println("AWS Error Code:   " + ase.getErrorCode());
	            System.out.println("Error Type:       " + ase.getErrorType());
	            System.out.println("Request ID:       " + ase.getRequestId());
	        } catch (AmazonClientException ace) {
	        	System.out.println("initializeQueues-ACEexception");
	            System.out.println("Caught an AmazonClientException, which means the client encountered " +
	                    "a serious internal problem while trying to communicate with SQS, such as not " +
	                    "being able to access the network.");
	            System.out.println("Error Message: " + ace.getMessage());
	        }
			
			System.out.println("Queues production begins.");
			try {
				Thread.currentThread().sleep(1000*60);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("Queues production completed.");
			return;
		}
		System.out.println("Queues have already been created.");
	}
	
	private static void terminateTheQueues(AmazonSQS mysqs) {
		String terminationqueue=FindtheterminationqueueURL(mysqs);
		List<String>listOfQueues=mysqs.listQueues().getQueueUrls();
		
		if(!listOfQueues.isEmpty()) {
			//Whit until the termination queue empty
			while(!KILLMEmessageArrived(mysqs, terminationqueue)) {
				try {
					Thread.currentThread().sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			//Delete all queues
			for (String queueUrl : mysqs.listQueues().getQueueUrls()) {
				mysqs.deleteQueue(new DeleteQueueRequest(queueUrl));
			}
		}
		System.out.println("Terminate the queues.");
	}
	
	private static boolean KILLMEmessageArrived(AmazonSQS mysqs,String terminationqueue) {
		ReceiveMessageRequest receiveMessageRequest= new ReceiveMessageRequest(terminationqueue);
		List<Message> messages= mysqs.receiveMessage(receiveMessageRequest).getMessages();
		if(!messages.isEmpty()) {
			mysqs.changeMessageVisibility(terminationqueue,messages.get(0).getReceiptHandle(),0);
			for(Message message:messages) {
				if(message.getBody().contains("kill me")) {
					return true;
				}
			}
		}
		return false;
	}
	
	
	private static void terminateTheManager(AmazonEC2 ec2, String managerInstanceId) {
		ArrayList<String> instanceIds = new ArrayList<String>();
		try {
			DescribeInstancesRequest request = new DescribeInstancesRequest();
		    DescribeInstancesResult response = ec2.describeInstances(request);
		    for(Reservation reservation : response.getReservations()) {
		        for(Instance instance : reservation.getInstances()) {
		  	  		String state=instance.getState().getName();				  	  	
	            	if((state.equals("running") || state.equals("pending")) && instance.getInstanceId().equals(managerInstanceId)){
	            		 instanceIds.add(managerInstanceId);		
		  	  		}
		        }
		    }
			
		} catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
		}
		
		try {
		    // Terminate instances.
			if(!instanceIds.isEmpty()) {
				TerminateInstancesRequest terminateRequest = new TerminateInstancesRequest(instanceIds);
				ec2.terminateInstances(terminateRequest);
			}
		} catch (AmazonServiceException e) {
		    // Write out any exceptions that may have occurred.
		    System.out.println("Error terminating instances");
		    System.out.println("Caught Exception: " + e.getMessage());
		    System.out.println("Reponse Status Code: " + e.getStatusCode());
		    System.out.println("Error Code: " + e.getErrorCode());
		    System.out.println("Request ID: " + e.getRequestId());
		}
		
		System.out.println("Terminate the Manager.");
	}
	
	private static String FindtheterminationqueueURL(AmazonSQS mysqs) {
		String terminationqueue="";
		List<String>listOfQueues=mysqs.listQueues().getQueueUrls();
		
		if(!listOfQueues.isEmpty()) {
			//Find the termination queue URL
			for (String queueUrl : mysqs.listQueues().getQueueUrls()) {
				if(queueUrl.contains("terminationqueue")){	
					terminationqueue=queueUrl;
				}
			}
		}
		return terminationqueue;
	}
	
	private static void terminateIfYouNeed(boolean terminate, AmazonEC2 ec2, String managerInstanceId, AmazonSQS mysqs) {	
		if(terminate) {
			System.out.println("Starts the termination process.");
			terminateTheQueues(mysqs);
			terminateTheManager(ec2,managerInstanceId);
    		System.out.println("Termination process completed.");	
			return;	
		}
		String terminationqueue = FindtheterminationqueueURL(mysqs);
		String Body="dead";
		mysqs.sendMessage(new SendMessageRequest(terminationqueue,Body));
		
	}
}
