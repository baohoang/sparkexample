package vn.wss.spark.model;

import java.io.Serializable;

public class Rating implements Serializable {
	private long id1;
	private long id2;
	private int order;

	public Rating() {
	}

	public Rating(long id1, long id2, int order) {
		this.id1 = id1;
		this.id2 = id2;
		this.order = order;
	}

	public long getId1() {
		return id1;
	}

	public void setId1(long id1) {
		this.id1 = id1;
	}

	public long getId2() {
		return id2;
	}

	public void setId2(long id2) {
		this.id2 = id2;
	}

	public int getOrder() {
		return order;
	}

	public void setOrder(int order) {
		this.order = order;
	}

}
