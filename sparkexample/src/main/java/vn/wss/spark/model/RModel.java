package vn.wss.spark.model;

import java.io.Serializable;

public class RModel implements Serializable {
	private long itemId;
	private long similarId;
	private int a;
	private int b;
	private int c;

	public RModel() {
	}

	public RModel(long itemId, long similarId, int a, int b, int c) {
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

	public int getA() {
		return a;
	}

	public void setA(int a) {
		this.a = a;
	}

	public int getB() {
		return b;
	}

	public void setB(int b) {
		this.b = b;
	}

	public int getC() {
		return c;
	}

	public void setC(int c) {
		this.c = c;
	}

	public RModel addA(RModel a) {
		RModel res = this;
		int A = a.getA() + res.getA();
		res.setA(A);
		return res;
	}

	public RModel addC(RModel a) {
		RModel res = this;
		int C = a.getC() + res.getC();
		res.setC(C);
		return res;
	}

}
