package vn.wss.spark.sql;

import java.io.Serializable;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

@Entity
@Table(name = "Product_Relation")
@NamedQueries({
		@NamedQuery(name = "RecommendedItem.findAll", query = "SELECT c FROM RecommendedItem c"),
		@NamedQuery(name = "RecommendedItem.removeAll", query = "DELETE FROM RecommendedItem WHERE relationType=2") })
public class RecommendedItem implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private long id;
	private long productid;
	private long productidrelation;
	private int stt;
	private int relationType;

	public RecommendedItem() {

	}

	public RecommendedItem(long idItem, long idRecommendedItem, int order) {
		super();
		this.productid = idItem;
		this.productidrelation = idRecommendedItem;
		this.stt = order;
		this.relationType = 2;
	}

	public int getOrder() {
		return stt;
	}

	public void setOrder(int order) {
		this.stt = order;
	}

	public int getRelationType() {
		return relationType;
	}

	public void setRelationType(int relationType) {
		this.relationType = relationType;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public long getIdItem() {
		return productid;
	}

	public void setIdItem(long idItem) {
		this.productid = idItem;
	}

	public long getIdRecommendedItem() {
		return productidrelation;
	}

	public void setIdRecommendedItem(long idRecommendedItem) {
		this.productidrelation = idRecommendedItem;
	}

	@Override
	public String toString() {
		return "RecommendedItem [id=" + id + ", idItem=" + productid
				+ ", idRecommendedItem=" + productidrelation + "]";
	}

}
