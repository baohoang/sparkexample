package vn.wss.spark.model;

import java.io.Serializable;

public class PModel implements Serializable {
	private long userID;
	private long itemID;

	public PModel() {
	}

	public PModel(long userID, long itemID) {
		this.userID = userID;
		this.itemID = itemID;
	}

	public long getUserID() {
		return userID;
	}

	public void setUserID(long userID) {
		this.userID = userID;
	}

	public long getItemID() {
		return itemID;
	}

	public void setItemID(long itemID) {
		this.itemID = itemID;
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		if (obj instanceof PModel) {
			PModel d = (PModel) obj;
			return d.getUserID() == userID && d.getItemID() == itemID;
		}
		return false;
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		int prime = 31;
		int result = 1;
		result = result * prime + Long.hashCode(userID);
		result = result * prime + Long.hashCode(itemID);
		return result;
	}

}
