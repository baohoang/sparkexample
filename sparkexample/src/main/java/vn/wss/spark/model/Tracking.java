package vn.wss.spark.model;

import java.io.Serializable;
import java.util.Date;

public class Tracking implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6172921806679918701L;
	Date at;
	String ip;
	String referer;
	String sessionId;
	String uri;
	String userId;

	public Tracking() {
	}

	public Tracking(Date at, String ip, String referer, String sessionId,
			String uri, String userId) {
		this.at = at;
		this.ip = ip;
		this.referer = referer;
		this.sessionId = sessionId;
		this.uri = uri;
		this.userId = userId;
	}

	@Override
	public String toString() {
		return String.format(
				"tracking{at = %s, ip = %s, uri = %s, user_id = %s }", at, ip,
				uri, userId);
	}

	public Date getAt() {
		return at;
	}

	public void setAt(Date at) {
		this.at = at;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getReferer() {
		return referer;
	}

	public void setReferer(String referer) {
		this.referer = referer;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

}
