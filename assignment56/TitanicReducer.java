package assignment56;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author Yogesh
 * Purpose: calculate gender wise the average age of the people died.
 */

public class TitanicReducer extends Reducer<ClassSurvived, Text, Text, IntWritable>{
	
	int count;
	String details;
	public void reduce(ClassSurvived key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		count = 0;
		for(Text value : values){
			count++;
			details = key + "," + value;
			context.write(new Text(details), new IntWritable(count));
		}
		
	}
	
}
