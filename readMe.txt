How to Run the jar file:
make sure hadoop and yarn are running
use following command to change directory to hadoop home
cd /home/student/hadoop-2.7.2
run the IntSum.jar using following command:

Path_to_IntSum.jar
Path_to_input.txt (input file must be only numbers separated by comma)(Input file must be located in the hdfs)
Path_to_output_dir (output directory should not exists!)

bin/hadoop jar Path_to_InputSum.jar Path_to_input.txt Path_to_output_dir

Example:

bin/hadoop jar /home/student/IntSum.jar /input/input1.txt /IntSumResult