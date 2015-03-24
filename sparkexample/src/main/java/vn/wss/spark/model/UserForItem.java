package vn.wss.spark.model;

import java.io.Serializable;
import java.util.List;

public class UserForItem implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Long idtem;
	private List<Long> listuser;

	public UserForItem() {
	}

	public UserForItem(Long idtem, List<Long> listuser) {
		this.idtem = idtem;
		this.listuser = listuser;
	}

	public Long getIdtem() {
		return idtem;
	}

	public void setIdtem(Long idtem) {
		this.idtem = idtem;
	}

	public List<Long> getListuser() {
		return listuser;
	}

	public void setListuser(List<Long> listuser) {
		this.listuser = listuser;
	}
}