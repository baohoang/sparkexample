package vn.wss.spark.model;

import java.io.Serializable;

public class VModel implements Serializable {
	private long id;
	private int numOfVisitors;

	public VModel() {
	}

	public VModel(long id, int numOfVisitors) {
		this.id = id;
		this.numOfVisitors = numOfVisitors;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public int getNumOfVisitors() {
		return numOfVisitors;
	}

	public void setNumOfVisitors(int numOfVisitors) {
		this.numOfVisitors = numOfVisitors;
	}

}
