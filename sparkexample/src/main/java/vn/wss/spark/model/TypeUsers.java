package vn.wss.spark.model;

import java.io.Serializable;
import java.util.ArrayList;

public class TypeUsers implements Serializable {
	private long itemid;
	private ArrayList<Long> listUsers;

	public TypeUsers() {
		listUsers = new ArrayList<>();
	}

	public TypeUsers(long itemid, ArrayList<Long> listUsers) {
		this.itemid = itemid;
		this.listUsers = listUsers;
	}

	public long getItemid() {
		return itemid;
	}

	public void setItemid(long itemid) {
		this.itemid = itemid;
	}

	public ArrayList<Long> getListUsers() {
		return listUsers;
	}

	public void setListUsers(ArrayList<Long> listUsers) {
		this.listUsers = listUsers;
	}

}
