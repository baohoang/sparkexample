package vn.wss.spark.model;

import java.io.Serializable;

public class TrackingModel implements Serializable{
	private String uri;
	private String user_id;
	
	public TrackingModel() {
	}

	public TrackingModel(String uri, String user_id) {
		this.uri = uri;
		this.user_id = user_id;
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
