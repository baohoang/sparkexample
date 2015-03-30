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
}
