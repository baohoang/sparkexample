package vn.wss.spark.model;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

public class LongList implements Serializable, Iterable<Long> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4253563167965866518L;
	private List<Long> list;

	public LongList() {
		super();
	}

	public LongList(List<Long> list) {
		this.list = list;
	}

	@Override
	public Iterator<Long> iterator() {
		// TODO Auto-generated method stub
		return list.iterator();
	}

	public List<Long> getList() {
		return list;
	}

	public void setList(List<Long> list) {
		this.list = list;
	}

	public int size() {
		return list.size();
	}

	public void setIndex(int index, long element) {
		list.set(index, element);
	}

	public long getIndex(int index) {
		return list.get(index);
	}

}
