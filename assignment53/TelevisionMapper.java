package assignment53;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author Yogesh
 * Purpose: Maps state wise sale of the company Onida.
 * 
 * MapOutput Key: State in Text
 * MapOutput Value: Company Name in Text
 */

public class TelevisionMapper extends Mapper<LongWritable, Text, Text, Text>{

	Text companyName;
	Text productName;
	Text state;
	Text cName;
	public static final Text NA = new Text("NA");
	
	public void setup(Context context){
		companyName = new Text();
		productName = new Text();
		state = new Text();
		cName = new Text("Onida");
	}
	
	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException{
		String[] lineArray = values.toString().split("\\|");
		
		companyName.set(lineArray[0]);
		productName.set(lineArray[1]);
		state.set(lineArray[3]);
		
		if(companyName.equals(cName) && !productName.equals(NA)){
			context.write(state, companyName);
		}
		
		
	}
}
