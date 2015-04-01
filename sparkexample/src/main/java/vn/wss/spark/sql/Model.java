package vn.wss.spark.sql;

import java.io.Serializable;

public class Model implements Serializable {
	private int ID;
	private long ProductID;
	private long ProductIDRelation;
	private int STT;
	private int RelationType;

	public Model(int iD, long productID, long productIDRelation, int sTT,
			int relationType) {
		ID = iD;
		ProductID = productID;
		ProductIDRelation = productIDRelation;
		STT = sTT;
		RelationType = relationType;
	}

	public Model() {
	}

	public int getID() {
		return ID;
	}

	public void setID(int iD) {
		ID = iD;
	}

	public long getProductID() {
		return ProductID;
	}

	public void setProductID(long productID) {
		ProductID = productID;
	}

	public long getProductIDRelation() {
		return ProductIDRelation;
	}

	public void setProductIDRelation(long productIDRelation) {
		ProductIDRelation = productIDRelation;
	}

	public int getSTT() {
		return STT;
	}

	public void setSTT(int sTT) {
		STT = sTT;
	}

	public int getRelationType() {
		return RelationType;
	}

	public void setRelationType(int relationType) {
		RelationType = relationType;
	}

}
