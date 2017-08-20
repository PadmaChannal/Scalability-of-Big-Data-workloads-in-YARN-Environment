import java.io.IOException;
    import java.util.*;
    import java.lang.String;
Import java.io.*;

    import org.apache.hadoop.fs.Path;
    import org.apache.hadoop.conf.*;
    import org.apache.hadoop.io.*;
    import org.apache.hadoop.mapreduce.*;
    import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
    import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
    import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
    import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;

    public class IntSum {

     public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    	 //static String IntSum;
    	 //private final static Text word = new Text("sum");
        private IntWritable output = new IntWritable();
       
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	int sum = 0;
        	//String line= value.toString();
        	String[] fields = StringUtils.split(value.toString(),'\\',',');
        	 //IntSum = conf.get("IntSum");
        	for(String j:fields){
        		sum=sum+Integer.parseInt(j);
        	}
        	 //output.set(Integer.parseInt(line));
        	//System.out.println(line+" ****");
        	  context.write(new Text("sum"),new IntWritable(sum) );
                
            
        }

	
     } 

     public static class Reduce extends Reducer<Text, Iterable<IntWritable>, Text, IntWritable> {
    	 //IntWritable result = new IntWritable();
    	 int sum = 0;
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
          throws IOException, InterruptedException {
            
            System.out.println(values.toString()+" ****values");
            for (IntWritable val : values) {
                sum += val.get();
            }
            //result.set(sum );
            context.write(key, new IntWritable(sum));
        }
     }

     public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //conf.set("IntSum", args[0]);
            Job job = new Job(conf);
            
        job.setOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setCombinerClass(Reduce.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setJarByClass(IntSum.class);
        job.waitForCompletion(true);
     }

    }
