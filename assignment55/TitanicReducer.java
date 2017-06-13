package assignment55;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author Yogesh
 * Purpose: calculate gender wise the average age of the people died.
 */

public class TitanicReducer extends Reducer<Text, FloatWritable, Text, FloatWritable>{

	float average;
	float totalEntries;
	float total;
	
	
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException{
		totalEntries = 0;
		total = 0;
		
		for(FloatWritable value : values){
			totalEntries++;
			total = total + value.get();
		}
		
		average = total/totalEntries;
		context.write(key, new FloatWritable(average));
	}
	
}
