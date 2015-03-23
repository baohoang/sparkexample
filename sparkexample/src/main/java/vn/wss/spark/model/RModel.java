package vn.wss.spark.model;

import java.io.Serializable;

public class RModel implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private long itemId;
	private long similarId;
	private long a;
	private long b;
	private long c;

	public RModel() {
	}

	public RModel(long itemId, long similarId, long a, long b, long c) {
		this.itemId = itemId;
		this.similarId = similarId;
		this.a = a;
		this.b = b;
		this.c = c;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "RModel [" + itemId + " " + similarId + " " + a + " " + b + " "
				+ c + "]";
	}

	public long getItemId() {
		return itemId;
	}

	public void setItemId(long itemId) {
		this.itemId = itemId;
	}

	public long getSimilarId() {
		return similarId;
	}

	public void setSimilarId(long similarId) {
		this.similarId = similarId;
	}

	public long getA() {
		return a;
	}

	public void setA(long a) {
		this.a = a;
	}

	public long getB() {
		return b;
	}

	public void setB(long b) {
		this.b = b;
	}

	public long getC() {
		return c;
	}

	public void setC(long c) {
		this.c = c;
	}

}
