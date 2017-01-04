package hadoop_master.hadoop.topkey;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TopWritable implements WritableComparable<TopWritable>{
    public String getWord() {
		return word;
	}

	public TopWritable(String word, Long count) {
		super();
		this.word = word;
		this.count = count;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}

	private String word;
    private Long count;
    
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.word=arg0.readUTF();
		this.count=arg0.readLong();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeUTF(word);
		arg0.writeLong(count);
	}

	@Override
	public int compareTo(TopWritable o) {
		// TODO Auto-generated method stub
		return (int) -(this.count-o.getCount());
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return super.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		return super.equals(obj);
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return word+"/t"+count;
	}
   
}
