package vn.wss.spark.model;

import java.io.Serializable;
import java.util.List;

public class LongList implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4253563167965866518L;
	private long[] list;
	private int size;

	public LongList() {
	}

	public LongList(int size) {
		this.size = size;
		this.list = new long[size];
	}

	public LongList(int size, long[] list) {
		this.size = size;
		this.list = list;
	}

	public LongList(List<Long> list) {
		this.size = list.size();
		this.list = new long[size];
		for (int i = 0; i < list.size(); i++) {
			this.list[i] = list.get(i);
		}
	}

	public long[] getList() {
		return list;
	}

	public void setList(long[] list) {
		this.list = list;
	}

	public void setIndex(int index, long element) {
		list[index] = element;
	}

	public long getIndex(int index) {
		return list[index];
	}

}
