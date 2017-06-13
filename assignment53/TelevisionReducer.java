package assignment53;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author Yogesh
 *
 * Purpose: Reduce and get results as state wise unit sale of particular company.
 * 
 * Output Key: State in Text.
 * Output Value: Number of units sold in each state.
 */

public class TelevisionReducer extends Reducer<Text, Text, Text, Text> {
	
	int count;
	
	public void setup(Context context) throws IOException, InterruptedException{
		count = 0;
		context.write(new Text("State"), new Text("Number of Units sold(Onida)"));
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		count = 0;
		
		for(Text value : values)
			count++;
		
		context.write(key, new Text(Integer.toString(count)));
		
	}
}
