package vn.websosanh.sparkexample;

import java.text.MessageFormat;

import java.io.Serializable;

public class Product implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Integer id;
	private String name;
	private String parents;

	public Product() {
	}

	public Product(Integer id, String name, String parents) {
		this.id = id;
		this.name = name;
		this.parents = parents;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getParents() {
		return parents;
	}

	public void setParent(String parent) {
		this.parents = parent;
	}

	@Override
	public String toString() {
		return MessageFormat.format(
				"Product'{'id={0}, name=''{1}'', parents={2}'}'", id, name,
				parents);
	}
}