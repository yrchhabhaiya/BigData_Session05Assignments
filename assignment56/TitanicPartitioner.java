package assignment56;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TitanicPartitioner extends Partitioner<ClassSurvived, Text>{

	@Override
	public int getPartition(ClassSurvived key, Text values, int arg2) {
		// TODO Auto-generated method stub
		
		if(key.IsSurvived)
			return 0;
		else
			return 1;
	}

}
