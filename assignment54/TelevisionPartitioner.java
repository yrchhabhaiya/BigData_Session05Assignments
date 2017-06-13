package assignment54;

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

public class TelevisionPartitioner extends Partitioner<CPSKey, Text> {

	final String[] reducer ={"Samsung","Onida","Akai","Lava","Zen"};
		
	@Override
	public int getPartition(CPSKey key, Text value, int arg2) {
		// TODO Auto-generated method stub
		String companyName = key.getCompanyName();
		
		if(companyName.equals(reducer[0]))
			return 0;
		else if(companyName.equals(reducer[1]))
			return 1;
		else if(companyName.equals(reducer[2]))
			return 2;
		else if(companyName.equals(reducer[3]))
			return 3;
		else
			return 4;
	}

}
