package vn.wss.spark.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class ArrayLongList implements Writable {

	private IntWritable size;
	private LongWritable[] arr;

	public ArrayLongList() {
	}

	public ArrayLongList(IntWritable size, LongWritable[] arr) {
		this.size = size;
		this.arr = arr;
	}

	public IntWritable getSize() {
		return size;
	}

	public void setSize(IntWritable size) {
		this.size = size;
	}

	public LongWritable[] getArr() {
		return arr;
	}

	public void setArr(LongWritable[] arr) {
		this.arr = arr;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		size.write(out);
		for (int i = 0; i < size.get(); i++) {
			arr[i].write(out);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		size = new IntWritable();
		size.readFields(in);
		arr = new LongWritable[size.get()];
		for (int i = 0; i < size.get(); i++) {
			LongWritable l = new LongWritable();
			l.readFields(in);
			arr[i] = l;
		}
	}

}
