package assignment52;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author Yogesh
 * TelevisionPartitioner.Java
 * Purpose: Custom partitioner on the basis of Company name. 
 * company name starts with A-F (upper or lower case) should go to 1st reducer,
 * those starting with G-L to 2nd reducer, those starting with M-R to 3rd reducer and others to 4th reducer
 * 
 * Input Key: Company of type Text
 * Input Values: Details of type Text.
 */

public class TelevisionPartitioner extends Partitioner<Text, Text> {

	String companyNameLower;
	final String reducer1 ="abcdef";
	final String reducer2 ="ghijkl";
	final String reducer3 ="mnopqr";
		
	@Override
	public int getPartition(Text key, Text value, int arg2) {
		// TODO Auto-generated method stub
		companyNameLower = key.toString().toLowerCase();
				
		if(reducer1.contains(companyNameLower.substring(0, 1)))
		return 0;
		else if(reducer2.contains(companyNameLower.substring(0, 1)))
		return 1;
		else if(reducer3.contains(companyNameLower.substring(0, 1)))
		return 2;
		else		
		return 3;
	}

}
