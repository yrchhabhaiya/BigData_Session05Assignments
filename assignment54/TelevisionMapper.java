package assignment54;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Yogesh
 * TelevisionMapper.Java
 * Purpose: Map company wise sale of televisions
 * 
 * MapOutput Key: CompanyName + ProductName + Size of type Text
 * MapOutput Values: Rest of record of type Text.
 */

public class TelevisionMapper extends Mapper<LongWritable, Text, CPSKey, Text>{

	Text companyName;
	Text productName;
	Text size;
	Text details;
	CPSKey cpsKey;
	public static final Text NA = new Text("NA");
	
	public void setup(Context context){
		companyName = new Text();
		productName = new Text();
		size = new Text();
		details = new Text();
		cpsKey = new CPSKey();
	}
	
	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException{
		String[] lineArray = values.toString().split("\\|");
		
		cpsKey.set(lineArray[0], lineArray[1], lineArray[2]);
		companyName.set(lineArray[0]);
		productName.set(lineArray[1]);
		
		details.set(lineArray[3] + "\\|" + lineArray[4] + "\\|" + lineArray[5]);
		if(!(companyName.equals(NA)) && !(productName.equals(NA)))
		context.write(cpsKey, details);
		}
		
}

