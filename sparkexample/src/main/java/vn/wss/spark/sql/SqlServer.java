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
		Map<String, String> options = new HashMap<String, String>();
		String url = "jdbc:sqlserver://183.91.14.82;databaseName=QT_2;username=qt_vn;password=@F4sJ=l9/ryJt9MT";
		options.put("url", url);
		options.put("dbtable", "SELECT * FROM [QT_2].[dbo].[Product_Relation]");
		//options.put("driver", "com.microsoft.jdbc.sqlserver.SQLServerDriver");
		DataFrame jdbcDF = sqlContext.load("jdbc", options);
		jdbcDF.printSchema();
		sc.stop();
	}
}
