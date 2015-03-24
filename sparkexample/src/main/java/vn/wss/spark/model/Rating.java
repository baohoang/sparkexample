package vn.wss.spark.model;

import java.io.Serializable;

public class Rating implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1796765202099313453L;
	private long id;
	private double rate;

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public double getRate() {
		return rate;
	}

	public void setRate(double rate) {
		this.rate = rate;
	}

	public Rating(long id, double rate) {
		this.id = id;
		this.rate = rate;
	}

	public Rating() {
	}

}
