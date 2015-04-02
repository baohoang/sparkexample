package vn.wss.spark.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.cassandra.CassandraSQLContext;

public class SqlServer {
	public static void main(String[] args) throws SQLException {
		SparkConf conf = new SparkConf(true);
		JavaSparkContext sc = new JavaSparkContext(conf);
		// DataFrame dataFrame=sqlContext.createDataFrame(rdd, beanClass);
		SqlConnection sqlConnection = SqlConnection.getInstance();
		String sql = "SELECT * FROM dbo.Product_Relation";
		PreparedStatement preparedStatement = sqlConnection.getConn()
				.prepareStatement(sql);
		ResultSet resultSet = preparedStatement.executeQuery();
		System.out.println(resultSet.getRow());
		sc.stop();
	}
}
