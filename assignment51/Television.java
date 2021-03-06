/**
 * Package contains Assignment 5 Task 1 from acadgild.
 */
package assignment51;

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
 * Purpose: use a custom partitioner with 4 reducers. Make sure that all records whose company name starts with A-F (upper or lower case) should go to 1st reducer,
 * those starting with G-L to 2nd reducer, those starting with M-R to 3rd reducer and others to 4th reducer.
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
		Job job = new Job(conf, "Assignment51");
		
		//Jar class to initiate the program
		job.setJarByClass(Television.class);
		
		//set mapper/reducer classes
		job.setMapperClass(TelevisionMapper.class);
		job.setReducerClass(TelevisionReducer.class);
		
		//set partitioner class
		job.setPartitionerClass(TelevisionPartitioner.class);
		
		//set number of reducers
		job.setNumReduceTasks(4);
		
		
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
