package assignment56;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @author Yogesh
 * Purpose: contains two parameters passenger class and whether they survived or not.
 */

public class ClassSurvived implements WritableComparable<ClassSurvived>{
	
	String pClass;
	boolean IsSurvived;
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.pClass = in.readUTF();
		this.IsSurvived = in.readBoolean();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.pClass);
		out.writeBoolean(this.IsSurvived);
	}
	@Override
	public int compareTo(ClassSurvived CS) {
		// TODO Auto-generated method stub
		int cmp = this.pClass.compareTo(CS.pClass);
			return cmp;
	}
	@Override
	public String toString(){
		String survived;
		if(IsSurvived) survived = "Survived";
		else survived = "Died";
		return (this.pClass + "," + survived);
	}

	
	
}
