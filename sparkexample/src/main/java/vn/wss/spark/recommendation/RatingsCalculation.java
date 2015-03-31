package vn.wss.spark.recommendation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class RatingsCalculation {
	private static final Logger logger = LogManager
			.getLogger(RatingsCalculation.class);

	public static void main(String[] args) {
		SparkConf conf = new SparkConf(true);
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		DataFrame rate = sqlContext.load("/spark/ratings/parquet");
		rate.toJavaRDD().foreach(new VoidFunction<Row>() {

			@Override
			public void call(Row t) throws Exception {
				// TODO Auto-generated method stub
				logger.info(t.getLong(3) + " " + t.getLong(4) + " "
						+ t.getInt(0) + " " + t.getInt(1) + " " + t.getInt(2));
			}
		});
	}
}
