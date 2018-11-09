# Assignment3-Hadoop
Hadoop assignment for CSCI 459 Networking.

To run this project:
-SSH/Putty into zoidberg cluster
-Move with an SCP tool like WinSCP the assignment3.java file, whatever directory you want
-Move with WinSCP the 3 test files file01, file02, file03, then move using hadoop fs -copyFromLocal into the input directory
-Compile with: bin/hadoop com.sun.tools.javac.Main assignment3
-Make into a jar with: jar cf as3.jar assignment3.class
-Run with: hadoop jar as3.jar assignment3 /inputdirectory/ /outputdirectory/
-View the results in the output directory.

