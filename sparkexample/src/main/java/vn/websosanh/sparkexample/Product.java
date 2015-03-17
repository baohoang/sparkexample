package vn.websosanh.sparkexample;

import java.io.Serializable;
import java.text.MessageFormat;

public class Product implements Serializable {
    /**
	 * 
	 */
	private static final long serialVersionUID = -5838232291344432959L;
	private Integer id;
    private String name;
    private Integer parent;

    public Product() { }

    public Product(Integer id, String name, Integer parent) {
        this.id = id;
        this.name = name;
        this.parent = parent;
    }

    public Integer getId() { return id; }
    public void setId(Integer id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public Integer getParent() { return parent; }
    public void setParents(Integer parent) { this.parent = parent; }

    @Override
    public String toString() {
        return MessageFormat.format("Product'{'id={0}, name=''{1}'', parents={2}'}'", id, name, parent);
    }
}