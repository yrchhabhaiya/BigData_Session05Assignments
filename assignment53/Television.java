package assignment53;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @author Yogesh
 * Television.Java
 * Purpose: use combiner (mini reducer) to make program efficient.
 * 
 * @param String parameter as hdfs path of input file & hdfs output directory.
 * 
 */
public class Television {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		//get configuration
		Configuration conf = new Configuration();
		
		//instantiate Job(Configuration, JobName)
		Job job = new Job(conf, "Assignment4.2_Task3");
		
		//Jar class to initiate the program
		job.setJarByClass(Television.class);
		
		//set mapper/reducer classes
		job.setMapperClass(TelevisionMapper.class);
		job.setReducerClass(TelevisionReducer.class);
		
		//set combiner class
		job.setCombinerClass(TelevisionReducer.class);
		
		
		//input ARGUMENTS to the program
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//input classes to Program
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//output classes of Mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//output classes of Reducer
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//Mapper Reducer Invocation
		job.waitForCompletion(true);
		
	}

}
