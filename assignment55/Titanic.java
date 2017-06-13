package assignment55;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/**
 * 
 * @author Yogesh
 * Purpose: Find the average age of males and females who died in the Titanic tragedy.
 * Dataset: Column 1 : PassengerId, Column 2 : Survived  (survived=0 & died=1), Column 3 : Pclass, Column 4 : Name, Column 5 : Sex, 
 * 			Column 6 : Age, Column 7 : SibSp, Column 8 : Parch, Column 9 : Ticket, Column 10 : Fare, Column 11 : Cabin, Column 12 : Embarked.
 * 
 * MapOutput Key: Sex in Text.
 * MapOutput Values: Age in IntWritable
 * 
 */
public class Titanic {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Assignment53");
		
		//set jar class.
		job.setJarByClass(Titanic.class);
		
		//set map key value classes.
		job.setMapOutputKeyClass(Text.class); //male or female
		job.setMapOutputValueClass(FloatWritable.class); //Age
		
		//set reduce key value classes.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		// input to progran
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//input output classes
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//set mapper reducer classes
		job.setMapperClass(TitanicMapper.class);
		job.setReducerClass(TitanicReducer.class);
		
		//invoke mapreduce
		job.waitForCompletion(true);
		
	}

}
