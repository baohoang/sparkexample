package vn.wss.spark.model;

import java.io.Serializable;

public class UserForItem implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Long idtem;
	private Iterable<Long> listuser;

	public UserForItem() {
	}

	public UserForItem(Long idtem, Iterable<Long> listuser) {
		this.idtem = idtem;
		this.listuser = listuser;
	}

	public Long getIdtem() {
		return idtem;
	}

	public void setIdtem(Long idtem) {
		this.idtem = idtem;
	}

	public Iterable<Long> getListuser() {
		return listuser;
	}

	public void setListuser(Iterable<Long> listuser) {
		this.listuser = listuser;
	}

}