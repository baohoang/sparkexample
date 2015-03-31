package vn.wss.spark.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TypeItems implements Serializable {
	private long userid;
	private List<Long> listItems;

	public TypeItems() {
		listItems = new ArrayList<>();
	}

	public TypeItems(long userid, List<Long> listItems) {
		this.userid = userid;
		this.listItems = listItems;
	}

	public long getUserid() {
		return userid;
	}

	public void setUserid(long userid) {
		this.userid = userid;
	}

	public List<Long> getListItems() {
		return listItems;
	}

	public void setListItems(List<Long> listItems) {
		this.listItems = listItems;
	}

}
