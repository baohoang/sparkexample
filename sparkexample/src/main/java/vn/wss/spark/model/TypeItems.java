package vn.wss.spark.model;

import java.io.Serializable;
import java.util.ArrayList;

public class TypeItems implements Serializable {
	private long userid;
	private ArrayList<Long> listItems;

	public TypeItems() {
		listItems = new ArrayList<>();
	}

	public TypeItems(long userid, ArrayList<Long> listItems) {
		this.userid = userid;
		this.listItems = listItems;
	}

	public long getUserid() {
		return userid;
	}

	public void setUserid(long userid) {
		this.userid = userid;
	}

	public ArrayList<Long> getListItems() {
		return listItems;
	}

	public void setListItems(ArrayList<Long> listItems) {
		this.listItems = listItems;
	}

}
