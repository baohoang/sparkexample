package vn.wss.spark.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class RatingWritable implements Writable {
	private LongWritable id;
	private DoubleWritable rate;

	public RatingWritable() {
	}
	

	public RatingWritable(LongWritable id, DoubleWritable rate) {
		this.id = id;
		this.rate = rate;
	}


	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		id.write(out);
		rate.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		id=new LongWritable();
		id.readFields(in);
		rate=new DoubleWritable();
		rate.readFields(in);
	}

	public LongWritable getId() {
		return id;
	}

	public void setId(LongWritable id) {
		this.id = id;
	}

	public DoubleWritable getRate() {
		return rate;
	}

	public void setRate(DoubleWritable rate) {
		this.rate = rate;
	}
	

}
