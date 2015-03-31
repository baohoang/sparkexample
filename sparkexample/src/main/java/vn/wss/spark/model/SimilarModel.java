package vn.wss.spark.model;

import java.io.Serializable;

public class SimilarModel implements Serializable {
	private long id1;
	private long id2;
	private int numOfSimilars;

	public SimilarModel() {
	}

	public SimilarModel(long id1, long id2, int numOfSimilars) {
		this.id1 = id1;
		this.id2 = id2;
		this.numOfSimilars = numOfSimilars;
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

	public int getNumOfSimilars() {
		return numOfSimilars;
	}

	public void setNumOfSimilars(int numOfSimilars) {
		this.numOfSimilars = numOfSimilars;
	}

}
