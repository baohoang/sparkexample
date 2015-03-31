package vn.wss.spark.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TModel implements Serializable {
	private long id;
	private String list;

	public TModel() {
	}

	public TModel(long id, String list) {
		this.id = id;
		this.list = list;
	}

	public TModel(long id, List<Long> list) {
		this.id = id;
		this.list = "";
		for (int i = 0; i < list.size(); i++) {
			this.list = list.get(i) + ",";
		}
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getList() {
		return list;
	}

	public void setList(String list) {
		this.list = list;
	}

	public void addList(long a) {
		if (list == null) {
			list = "";
		}
		list += a + "";
	}

	public TypeModel convert() {
		List<Long> listArr = new ArrayList<>();
		String[] a = list.split(",");
		for (int i = 0; i < a.length; i++) {
			listArr.add(Long.parseLong(a[i]));
		}
		return new TypeModel(id, listArr);
	}
}
