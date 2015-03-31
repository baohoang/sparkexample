package vn.wss.spark.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TypeModel implements Serializable {
	private long id;
	private List<Long> list;

	public TypeModel() {
	}

	public TypeModel(long itemid, List<Long> listUsers) {
		this.id = itemid;
		this.list = listUsers;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public List<Long> getList() {
		return list;
	}

	public void setList(List<Long> list) {
		this.list = list;
	}

	public void addList(long a) {
		if (list == null) {
			list = new ArrayList<>();
		}
		list.add(a);
	}

	public TModel convert() {
		String lString = "";
		for (long a : list) {
			lString += a + ",";
		}
		return new TModel(id, lString);
	}
}
