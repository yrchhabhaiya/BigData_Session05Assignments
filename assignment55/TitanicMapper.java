package assignment55;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
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
public class TitanicMapper extends Mapper<LongWritable, Text, Text, FloatWritable>{
	
	Text sex;
	FloatWritable age;
	Text survived;
	Text dead;
	
	public void setup(Context context){
		sex = new Text();
		age = new FloatWritable();
		survived = new Text();
		dead = new Text("0");
	}
	
	
	public void map(LongWritable Key, Text values, Context context) {
		
		String[] lineArray = values.toString().split(",");
		
		sex.set(lineArray[4]);
		if(!lineArray[5].equals(""))
		age.set(Float.parseFloat(lineArray[5]));
		
		survived.set(lineArray[1]);
		
		if(survived.equals(dead) && !lineArray[5].equals(""))	//data cleaning
			try {
				context.write(sex, age);
			} catch (IOException e) {
				System.out.println("Input Output Exception Triggered");
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
}
