package vn.wss.spark.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TypeUsers implements Serializable {
	private long itemid;
	private List<Long> listUsers;

	public TypeUsers() {
		listUsers = new ArrayList<>();
	}

	public TypeUsers(long itemid, List<Long> listUsers) {
		this.itemid = itemid;
		this.listUsers = listUsers;
	}

	public long getItemid() {
		return itemid;
	}

	public void setItemid(long itemid) {
		this.itemid = itemid;
	}

	public List<Long> getListUsers() {
		return listUsers;
	}

	public void setListUsers(List<Long> listUsers) {
		this.listUsers = listUsers;
	}

}
