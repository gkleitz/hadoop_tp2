#TP2_Hadoop

This repository contains the src folder for Hadoop course : lab2.
Every task is contained in the class "TP2_Hadoop".

This class contains the default mapper and reducer classes provided at 
https://hadoop.apache.org/docs/r2.7.1/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Example:_WordCount_v1.0
These default classes were renamed "defaultMapper" "and defaultReducer".

The first task (count names by origin) uses the "customMapper1" map class instead (+defaultReduce).
The second task (count number of names by number of origin) uses the "customMapper2" map class instead (+defaultReduce).
The third task (count percentage of names by gender) uses the "customMapper3" map class and the "customReducer3" reduce class instead.

To complete each task the classes set in the main() function should be changed accordingly.