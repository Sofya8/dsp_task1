# dsp_task1
Amazon Reviews Sarcasm Analysis

Assignment guidelines: https://www.cs.bgu.ac.il/~dsp181/Assignments/Assignment_1


Instructions:

1.If you use not our credentials, open a bucket named "michaelandsofibucket" and upload there the jars of the manager and the worker.

2.Change the path in the function "createHtmlFile" in "RecieverDownloader" class  to the path where the input and output files are on your computer

3.Run the program 


How our progrem works:
A localapp checks if there is a manager node, if not, it starts a manager. than he uploads the input file to s3 and sends messages with the 
input file adresses to the manager via the sqs queues.
The manager gets the adress, downloads a file from the s3, distributes it to mini tasks and sends them to the workers via the sqs.
After a worker get a message and resolves the mini-task, he sends the solution to the manager via the sqs.
The manager collects all the solutions of the mini-tasks, and uploads them as a output object to the s3. It will send a message with the adress of the 
file to the local app via sqs.
The local app gets the adress, downloads the object and makes an html output file.

We used for the manager: t2.micro, ami-6057e21a.
We used for the workers: t2.medium, ami-6057e21a.
We tried to run the application many times with different files, amount of files and different n-s.
For running 5 input files with n=100 it took 12 minutes. Which include the creation and deletion of instances and queues.
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
1)
Q:Did you think for more than 2 minutes about security? Do not send your credentials in plain text!
A:Of course we did! We never upload our credentials! We only use it when we do the bootstrap. We use environment variable.


2)
Q:Did you think about scalability? Will your program work properly when 1 million clients connected at the same time? 
	How about 2 million? 1 billion? Scalability is very important aspect of the system, be sure it is scalable!

A:We did. While the manager sends mini-tasks to the workers, he also collocets their resolvs to output and take care about plenty requestes of LocalApps in parallel.
	Our program will work very slowly when we will have 1 or 2 million clients, but if we will have an option to have more then t2.micro\t2.medium, we can 
	make a few little changes(like using a masive threadpool and not a small one, like we have and we would use it like the Reactor pattern as we have learned in the course "Systems Programming" ) 
	and make our program run faster with this kind of clients amount.


3)
Q:What about persistence? What if a node dies? What if a node stalls for a while? 
	Have you taken care of all possible outcomes in the system? Think of more possible issues that might arise from failures. What did you do to solve it? What about broken communications? Be sure to handle all fail-cases!

A:Our program checks every time how many workers are alive, and adds the appropriate amount of workers for the n paramiter and we delete a message in the worker only after it sent the solution to the Manager.
	We obviously thought of many other cases where the program could fail and we took care of them and we can elaborate on them in the meeting.


4)
Q:Threads in your application, when is it a good idea? When is it bad? Invest time to think about threads in your application!

A:We have a use of more the one thread in:
	local app:
		One thread that responsible for uploading the input files and sending the message to the manager with the adresses(bucket,key,localAppId
		and output file name) of the file in the s3.
		One thread that responsible for recieving the messages with the adresses of the S3 objects(output file), downloads it and makes a proper
		html file output.
	
	manager:
		One thread that responsable for recieving messages with the adresses of the input files in the S3, downloads the file, distributes it to
		mini-tasks, and sends them to the workers.
		Another thread that responsable for collecting the solutions of the mini-tasks, builds an output objects, uploads it to the S3, and sends
		the adress of the message to the local app.
		And Another One that use them both and alocat them for every localApp requeste file all the combination deales correctly for each of the file the localApps send.


5)
Q:Did you run more than one client at the same time? Be sure they work properly, and finish properly, and your results are correct.

A:We did.


6)
Q:Do you understand how the system works? Do a full run using pen and paper, draw the different parts and the communication that happens between them.

A:Done! Why is it not here? Because we used pen and paper! (Siriously, if we need to submit that too, we will. Just didn't thought we should really do it.)


7)
Q:Did you manage the termination process? Be sure all is closed once requested!

A:We did.


8)
Q:Did you take in mind the system limitations that we are using? Be sure to use it to its fullest!

A:As you can see, we took in mind the system limitations, for example the t2.micro and t2.medium, or the number of the instances that we can use via amazon serveice.


9)
Q:Are all your workers working hard? Or some are slacking? Why?

A:They do. Did not find that kind of problem, thay all do arund 100 messages per worker maybe one of them not,like you can see in the statistics file we submithed.


10)
Q:Is your manager doing more work than he's supposed to? Have you made sure each part of your system has properly defined tasks? 
	Did you mix their tasks? Don't!

A:No! We tried to do it suffitioned as possible. we made sure each part of your system has properly defined tasks and we didn't mix their tasks.


Lastly, are you sure you understand what distributed means? Is there anything in your system awaiting another?

We are sure we understand what distributed means.
		
