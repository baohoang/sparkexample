package vn.wss.spark.recommendation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class SparkSQLExample {
	private static final String FILE_PATH = "/spark";
	private static final String USER_ITEM = FILE_PATH + "/user4item";
	private static final String RESULT_PATH = FILE_PATH + "/result";

	private static final Logger logger = LogManager
			.getLogger(SparkSQLExample.class);

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		DataFrame parquetFile = sqlContext.parquetFile("/spark/rawdata/parquet");
		parquetFile.registerTempTable("pair_user_item");
	}
}
