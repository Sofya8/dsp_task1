package com.amazonaws.manager;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;

public class MessageProcessor implements Runnable {
	
	
	private AmazonEC2 ec2;
	private AmazonSQS mysqs;
	private AmazonS3 s3;
	private String AtoM;
	private String MtoW;
	private String WtoM;
	private String MtoA;
	private ConcurrentHashMap<String,Integer> apps;
	private int numOfJobsPerWorker;
	private AWSStaticCredentialsProvider credentialsProvider;
	private ConcurrentLinkedQueue<String> workerIds;
	private  Message message;
	
	public MessageProcessor(AmazonEC2 ec2,AmazonSQS mysqs,AmazonS3 s3 ,String AtoM,String MtoW,String WtoM,String MtoA,ConcurrentHashMap<String,Integer> apps,
			int numOfJobsPerWorker,AWSStaticCredentialsProvider credentialsProvider,ConcurrentLinkedQueue<String> workerIds,Message message) {
	
		this.ec2=ec2;
		this.mysqs=mysqs;
		this.s3=s3;
		this.AtoM=AtoM;
		this.MtoW=MtoW;
		this.WtoM=WtoM;
		this.MtoA=MtoA;
		this.apps=apps;
		this.numOfJobsPerWorker=numOfJobsPerWorker;
		this.credentialsProvider=credentialsProvider;
		this.workerIds=workerIds;
		this.message=message;
	}
	
	public void run() {

		 //start to take care on one of the messages
		FromApp fromApp=new FromApp(AtoM,MtoW, mysqs,s3,message);
		ToApp toApp=new ToApp((FromApp) fromApp,WtoM,MtoA, mysqs,s3);
		Thread t1=new Thread(fromApp);
		Thread t2=new Thread(toApp);
		
		//FromApp start
		t1.start();
		try {
			//FromApp done its job
			t1.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		if(apps.containsKey(fromApp.localAppName)) {
			apps.put(fromApp.localAppName, apps.remove(fromApp.localAppName)+1);
		}
		else {
			apps.put(fromApp.localAppName, 1);
		}
		
		//making Workers if we need
		workerIds.addAll(runNewWorker(fromApp.numOfReviews/numOfJobsPerWorker,ec2,credentialsProvider));
		
		//ToApp start
		t2.start();
		try {
			//ToApp done its job
			t2.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}		
	}
	
	
	
	
	private  List<String> runNewWorker(int numWorkersOpen, AmazonEC2 ec2, AWSCredentialsProvider credentialsProvider) {
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
	
	
	private  String getuserDataWorker(AWSCredentialsProvider credentialsProvider) {
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
	
	
	
	private  int workersRunning(AmazonEC2 ec2) {
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
	
	
	
	
	
}
