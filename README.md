#TP2_Hadoop

This repository contains the src folder for Hadoop course : lab2.
Code for the first task is contained in the "TP2_Hadoop" class.
Code for the first task is contained in the "Task2" class.
Code for the first task is contained in the "Task3" class.

These classes contain the default mapper and reducer classes provided at 
https://hadoop.apache.org/docs/r2.7.1/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Example:_WordCount_v1.0
These default classes were renamed "defaultMapper" "and defaultReducer".

The first task (count names by origin) uses the "customMapper1" map class instead (+defaultReduce).
The second task (count number of names by number of origin) uses the "customMapper2" map class instead (+defaultReduce).
The third task (count percentage of names by gender) uses the "customMapper3" map class and the "customReducer3" reduce class instead.

The commands I used to execute each task on the cluster are:

For task 1
	hadoop jar TP2_Hadoop.jar TP2_Hadoop /res/prenoms.csv /user/gkleitz/task1
	
For task 2
	hadoop jar TP2_Hadoop.jar task2 /res/prenoms.csv /user/gkleitz/task2
	
For task 3
	hadoop jar TP2_Hadoop.jar task3 /res/prenoms.csv /user/gkleitz/task3
	