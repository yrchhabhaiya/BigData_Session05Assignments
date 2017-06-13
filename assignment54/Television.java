/**
 * Package contains Assignment 5 Task 1 from acadgild.
 */
package assignment54;

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
 * Purpose:  Map Reduce program to view the total sales for each product for every Company corresponding to each size.
 * Make sure that all records for a single company goes to a single reducer and inside every reducer,
 * keys must be sorted in descending order of the size.
 * You may write a custom WritableComparable for this purpose. 
 * 
 * @param String parameter as hdfs path of input file & hdfs output directory.
 * Example recods:
 * Samsung|Optima|14|Madhya Pradesh|132401|14200  
 * Company Name|Product Name|Size in inches|State|Pin Code|Price 
 */

public class Television {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		//get configuration
		Configuration conf = new Configuration();
		
		//instantiate Job(Configuration, JobName)
		Job job = new Job(conf, "Assignment");
		
		//Jar class to initiate the program
		job.setJarByClass(Television.class);
		
		//set mapper/reducer classes
		job.setMapperClass(TelevisionMapper.class);
		job.setReducerClass(TelevisionReducer.class);
		
		//set partitioner class
		job.setPartitionerClass(TelevisionPartitioner.class);
		
		//set number of reducers
		job.setNumReduceTasks(5);		//Total Companies in sample file is 5
		
		
		//input ARGUMENTS to the program
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//input classes to Program
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//output classes of Mapper
		job.setMapOutputKeyClass(CPSKey.class);
		job.setMapOutputValueClass(Text.class);
		
		//output classes of Reducer
		job.setOutputKeyClass(CPSKey.class);
		job.setOutputValueClass(IntWritable.class);
		
		//Mapper Reducer Invocation
		job.waitForCompletion(true);
		
	}

}
