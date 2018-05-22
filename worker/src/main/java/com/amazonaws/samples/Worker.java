package com.amazonaws.samples;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

public class Worker {
	
	private static AmazonSQS mysqs;
	private static String urlWtoM;
	private static String urlMtoW;
	private static  int howMuchMessagesThisWorkerWorkedOn=0;
	public static void main(String args[]) {
		
		AWSCredentials myCredentials= new EnvironmentVariableCredentialsProvider().getCredentials();
		AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(myCredentials); 
		 mysqs = AmazonSQSClientBuilder.standard()
	                .withCredentials(credentialsProvider)
	                .withRegion("us-east-1")
	                .build();
		 for (String queueUrl : mysqs.listQueues().getQueueUrls()) {
			 	if(queueUrl.contains("MtoW"))
			 		urlMtoW=queueUrl;
			 	else if(queueUrl.contains("WtoM"))
			 		urlWtoM=queueUrl;
         }
		 List<Message> messages;
			int sentiment;
			String entities;
			boolean sarcastic;
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			//runs until the manager terminates this instance
			while(true) {	
					messages=getReviewsFromQueue();
						for(Message message : messages) {
							if(message.getBody().equals("Prepare to die!")) {
								dyingProcess(message);
								mysqs.deleteMessage(new DeleteMessageRequest(urlMtoW , message.getReceiptHandle()));
								Thread.currentThread().interrupt();
								return;
							}
							howMuchMessagesThisWorkerWorkedOn=howMuchMessagesThisWorkerWorkedOn+1;
							DspReview review = gson.fromJson(message.getBody(),
												DspReview.class);
							sentiment=findSentiment(review.getText());
							entities=getEntities(review.getText());
							sarcastic=isSarcastic(sentiment, review);
							System.out.println("sentiment: "+sentiment);
							System.out.println("entities: "+entities);
							System.out.println("sarcastic: "+sarcastic);
							System.out.println("BucketName: "+message.getMessageAttributes().get("BucketName").getStringValue());
							
							sendTheSolAndKillTheMessage(message,review, sentiment,
									entities, sarcastic);
						}
					
			}
		
	}
	
	
	private static void dyingProcess(Message message) {
		String body="Worker "+message.getMessageAttributes().get("WorkerId").getStringValue()+": "+ howMuchMessagesThisWorkerWorkedOn +".";
		mysqs.sendMessage(new SendMessageRequest(urlWtoM,body));
	}
	
	
	private static List<Message> getReviewsFromQueue() {
		try {
			
			 ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(urlMtoW);
	         List<Message> mes = mysqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();
	         return mes;
			
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
		
		
		return null;
		
	}
	
	
	private static int findSentiment(String review) {
		if(review.split(" ").length>99)
			return 2;
		Properties props = new Properties();
		props.put("annotators", "tokenize, ssplit, parse, sentiment");
		StanfordCoreNLP  sentimentPipeline =  new StanfordCoreNLP(props);
		 int mainSentiment = 0;
	        if (review!= null && review.length() > 0) {
	            int longest = 0;
	            Annotation annotation = sentimentPipeline.process(review);
	            for (CoreMap sentence : annotation
	                    .get(CoreAnnotations.SentencesAnnotation.class)) {
	                Tree tree = sentence
	                        .get(SentimentCoreAnnotations.AnnotatedTree.class);
	                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
	                String partText = sentence.toString();
	                if (partText.length() > longest) {
	                    mainSentiment = sentiment;
	                    longest = partText.length();
	                }
	 
	            }
	        }
	        return mainSentiment;
	}
	
	private static String getEntities(String review) {
		boolean flag=false;
		if(review.split(" ").length>99)
			return "";
		Properties props = new Properties();
		props.put("annotators", "tokenize , ssplit, pos, lemma, ner");
		StanfordCoreNLP NERPipeline =  new StanfordCoreNLP(props);
		
		 // create an empty Annotation just with the given text
        Annotation document = new Annotation(review);
 
        // run all Annotators on this text
        NERPipeline.annotate(document);
 
        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);
        String ans="[";
        for(CoreMap sentence: sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
                // this is the text of the token
                String word = token.get(TextAnnotation.class);
                // this is the NER label of the token
                String ne = token.get(NamedEntityTagAnnotation.class);
                if(ne.equals("PERSON")||ne.equals("LOCATION")||ne.equals("ORGANIZATION")){
                	flag=true;
	                ans=ans+ word + ":" + ne+", ";
	                //System.out.println("\t-" + word + ":" + ne);
                }
	        }
	    }
	    if(flag){    
	    	ans=ans.substring(0, ans.length()-2)+"]";
	    }else{
	    	ans=ans+"]";
	    }
	    return ans;
            
		
	}
	
	private static boolean isSarcastic(int sentiment, DspReview review ) {
		int num = Math.abs(Integer.valueOf(review.getRating())-sentiment);
		if(num>=4) return true;
		else return false;
	}
	


	private static void sendTheSolAndKillTheMessage(Message message,
			DspReview review,int sentiment, String entities,boolean sarcastic ) {
		try {
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			String jsonSol = gson.toJson(new DspSolution(review.getText(),review.getLink(), review.getRating(),sentiment,
					entities,sarcastic));
			
			String bucketName= message.getMessageAttributes().get("BucketName").getStringValue();
			String fileName= message.getMessageAttributes().get("FileName").getStringValue();
			String localAppName= message.getMessageAttributes().get("LocalAppName").getStringValue();
			String outputFileName= message.getMessageAttributes().get("OutputFileName").getStringValue();
			Map<String,MessageAttributeValue> attributes = new HashMap<String,MessageAttributeValue>();
			attributes.put("BucketName", new MessageAttributeValue().withDataType("String").withStringValue(bucketName));
			attributes.put("FileName", new MessageAttributeValue().withDataType("String").withStringValue(fileName));
			attributes.put("LocalAppName", new MessageAttributeValue().withDataType("String").withStringValue(localAppName));
			attributes.put("OutputFileName", new MessageAttributeValue().withDataType("String").withStringValue(outputFileName));
			
			SendMessageRequest smr=new SendMessageRequest(urlWtoM ,jsonSol).withMessageAttributes(attributes);
			mysqs.sendMessage(smr);
		
			//delete the JSON string\message from M->W's SQS
			mysqs.deleteMessage(new DeleteMessageRequest(urlMtoW , message.getReceiptHandle()));
	
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

}
