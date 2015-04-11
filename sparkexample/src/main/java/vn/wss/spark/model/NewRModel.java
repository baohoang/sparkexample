package vn.wss.spark.model;

import java.io.Serializable;

public class NewRModel implements Serializable {
	private long id1;
	private long id2;

	public NewRModel() {
	}

	public NewRModel(long id1, long id2) {
		this.id1 = id1;
		this.id2 = id2;
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (id1 ^ (id1 >>> 32));
		result = prime * result + (int) (id2 ^ (id2 >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof NewRModel) {
			NewRModel other = (NewRModel) obj;
			return (other.getId1() == id1 && other.getId2() == id2);
		}
		return false;
	}

}
