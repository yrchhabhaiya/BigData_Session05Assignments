package assignment54;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * @author Yogesh
 * TelevisionReducer.Java
 * Purpose: getting the count of units sold of each company.
 * 
 * ReduceOutput Key: Company of type Text
 * ReduceOutput Values: Number of units sold of type IntWritable.
 */
public class TelevisionReducer extends Reducer<CPSKey, Text, CPSKey, IntWritable> {
	
	int count;
	
	public void reduce(CPSKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		count = 0;
		
		for(Text value : values)
			count++;
		
		context.write(key, new IntWritable(count));
	}
}
