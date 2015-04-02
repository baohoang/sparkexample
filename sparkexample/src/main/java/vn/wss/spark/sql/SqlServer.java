package vn.wss.spark.sql;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class SqlServer {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf(true);
		conf.set("spark.executor.extraClassPath", "/home/hdspark/jtds-1.3.1.jar");
		conf.setJars(new String[]{"/home/hdspark/jtds-1.3.1.jar"});
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		String url = "jdbc:jtds:sqlserver://183.91.14.82:1433/QT_2";
		String username = "qt_vn";
		String password = "@F4sJ=l9/ryJt9MT";
		url += "?username=" + username + "&password=" + password;
		String driver = "net.sourceforge.jtds.jdbc.Driver";
		String table="[QT_2].[dbo].[Product_Relation]";
		Map<String, String> options = new HashMap<>();
		options.put("url", url);
		options.put("driver", driver);
		options.put("dbtable", table);
		DataFrame jdbcDF = sqlContext.load("jdbc", options);
		jdbcDF.printSchema();
		sc.stop();
	}
}
