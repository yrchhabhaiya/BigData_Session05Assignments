package assignment56;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 
 * @author Yogesh
 * 
 * Purpose: Map sex and age from given dataset.
 *  
 */
public class TitanicMapper extends Mapper<LongWritable, Text, ClassSurvived, Text>{
	
	boolean survived;
	ClassSurvived cs;
	String details;
	
	public void setup(Context context){
		survived = false;
		cs = new ClassSurvived();
	}
	
	
	public void map(LongWritable Key, Text values, Context context) throws IOException, InterruptedException {
		
		String[] lineArray = values.toString().split(",");
		
		if(lineArray[1].equals("0"))
			survived = true;
		else if(lineArray[1].equals("1"))
			survived = false;
		
		cs.pClass = lineArray[2];
		cs.IsSurvived = survived;
		details = lineArray[4] + "," + lineArray[5];
		if(!lineArray[5].equals(""))	//data cleaning
			context.write(cs, new Text(details));
	}
}
