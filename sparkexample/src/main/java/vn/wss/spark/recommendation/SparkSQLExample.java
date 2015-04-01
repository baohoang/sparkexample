package vn.wss.spark.recommendation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class SparkSQLExample {
	private static final Logger logger = LogManager
			.getLogger(SparkSQLExample.class);

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		DataFrame data = sqlContext.load("/spark/result/parquet");
		//get from raw data 
		data.toJavaRDD().foreach(new VoidFunction<Row>() {
			
			@Override
			public void call(Row t) throws Exception {
				// TODO Auto-generated method stub
				logger.info(t.toString());
			}
		});
		sc.stop();
	}

}
