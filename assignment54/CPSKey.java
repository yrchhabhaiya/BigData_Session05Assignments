package assignment54;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class CPSKey implements WritableComparable<CPSKey>{
	private String companyName;
	private String productName;
	private String size;
	
	
	public String getCompanyName() {
		return companyName;
	}

	public void setCompanyName(String companyName) {
		this.companyName = companyName;
	}

	public String getProductName() {
		return productName;
	}

	public void setProductName(String productName) {
		this.productName = productName;
	}

	public String getSize() {
		return size;
	}

	public void setSize(String size) {
		this.size = size;
	}

	
	
	public void set(String companyName, String productName, String size) {
		// TODO Auto-generated constructor stub
		this.companyName = companyName;
		this.productName = productName;
		this.size = size;
	}
	@Override
	public int compareTo(CPSKey key) {
		// TODO Auto-generated method stub
		
		int cmp1 = companyName.compareTo(key.companyName);
			if (cmp1 != 0)
				return cmp1;
			
		int cmp2 = productName.compareTo(key.productName.toString());
			if(cmp2 != 0)
				return cmp2;
			
		int cmp3 = size.compareTo(key.size);
			if(cmp3 != 0)
				return cmp3;
				
		return 0;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		companyName = in.readUTF();
		productName = in.readUTF();
		size = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(companyName);
		out.writeUTF(productName);
		out.writeUTF(size);
	}
	
	@Override
	public String toString(){
		return (this.companyName + "\\|" + this.productName + "\\|" + this.size);
	}

	 @Override
	    public int hashCode(){
	        return Integer.parseInt(companyName);
	    }
	 
	    @Override
	    public boolean equals(Object o)
	    {
	        if(o instanceof CPSKey)
	        {
	        	CPSKey cpsKey = (CPSKey) o;
	            return (cpsKey.companyName.equals(this.companyName) && cpsKey.productName.equals(this.productName) && cpsKey.size.equals(this.size));
	        }
	        return false;
	    }
	  
}
