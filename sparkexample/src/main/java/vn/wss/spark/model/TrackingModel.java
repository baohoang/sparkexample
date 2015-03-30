package vn.wss.spark.model;

import java.io.Serializable;
import java.util.Date;

public class TrackingModel implements Serializable{
	private Date at;
	private String uri;
	private String user_id;
	
	public TrackingModel() {
	}

	public TrackingModel(Date at, String uri, String user_id) {
		this.at = at;
		this.uri = uri;
		this.user_id = user_id;
	}

	public Date getAt() {
		return at;
	}

	public void setAt(Date at) {
		this.at = at;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public String getUser_id() {
		return user_id;
	}

	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}
	
}
