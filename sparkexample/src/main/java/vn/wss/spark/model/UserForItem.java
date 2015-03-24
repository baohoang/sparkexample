package vn.wss.spark.model;

import java.io.Serializable;

public class UserForItem implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Long idtem;
	private LongList listuser;

	public UserForItem() {
	}

	public UserForItem(Long idtem, LongList listuser) {
		this.idtem = idtem;
		this.listuser = listuser;
	}

	public Long getIdtem() {
		return idtem;
	}

	public void setIdtem(Long idtem) {
		this.idtem = idtem;
	}

	public LongList getListuser() {
		return listuser;
	}

	public void setListuser(LongList listuser) {
		this.listuser = listuser;
	}
}