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
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		String url = "jdbc:sqlserver://183.91.14.82;databaseName=QT_2";
		String username="qt_vn";
		String password="@F4sJ=l9/ryJt9MT";
		url+=";username="+username+";password="+password;
		String driver="com.microsoft.sqlserver.jdbc.SQLServerDriver";
		Map<String, String> options=new HashMap<>();
		options.put("url", url);
		options.put("driver", driver);
		options.put("dbtable","[QT_2].[dbo].[Product_Relation]");
		DataFrame jdbcDF = sqlContext.load("jdbc", options);
		jdbcDF.printSchema();
		sc.stop();
	}
}
