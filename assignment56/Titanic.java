package assignment56;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/**
 * 
 * @author Yogesh
 * Purpose: Find the number of people died or survived in each class with their genders and ages. 
 * Dataset: Column 1 : PassengerId, Column 2 : Survived  (survived=0 & died=1), Column 3 : Pclass, Column 4 : Name, Column 5 : Sex, 
 * 			Column 6 : Age, Column 7 : SibSp, Column 8 : Parch, Column 9 : Ticket, Column 10 : Fare, Column 11 : Cabin, Column 12 : Embarked.
 * 
 * MapOutput Key: Pclass in Text.
 * MapOutput Values: gender + age in Text.
 * 
 */
public class Titanic {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Assignment");
		
		//set jar class.
		job.setJarByClass(Titanic.class);
		
		//set map key value classes.
		job.setMapOutputKeyClass(ClassSurvived.class); //Pclass & survived
		job.setMapOutputValueClass(Text.class); //Gender + Age
		
		//set reduce key value classes.
		job.setOutputKeyClass(ClassSurvived.class);
		job.setOutputValueClass(Text.class);
		
		// input to progran
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setPartitionerClass(TitanicPartitioner.class);
		job.setNumReduceTasks(2);
		
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
