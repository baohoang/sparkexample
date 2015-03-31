package vn.wss.spark.recommendation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;
import vn.wss.spark.model.RModel;

public class SparkSQLExample {
	private static final String FILE_PATH = "/spark";
	private static final Logger logger = LogManager
			.getLogger(SparkSQLExample.class);

	public static void main(String[] args) {
//		SparkConf conf = new SparkConf();
//		JavaSparkContext sc = new JavaSparkContext(conf);
//		SQLContext sqlContext = new SQLContext(sc);
//		DataFrame data = sqlContext.load("/spark/rawdata/parquet");
//		//get from raw data 
//		JavaPairRDD<Long, Long> rawData = data.toJavaRDD().mapToPair(
//				new PairFunction<Row, Long, Long>() {
//
//					@Override
//					public Tuple2<Long, Long> call(Row t) throws Exception {
//						// TODO Auto-generated method stub
//						long key = t.getLong(0);
//						long val = t.getLong(1);
//						return new Tuple2<Long, Long>(key, val);
//					}
//				});
//		JavaPairRDD<Long, Long> a = RecommendationSpark.calculate(rawData);
//		JavaPairRDD<Tuple2<Long, Long>, Long> c = RecommendationSpark
//				.calculateSimilar(rawData);
//		JavaRDD<RModel> res = RecommendationSpark.fusion(c, a);
//		DataFrame schema = sqlContext.createDataFrame(res, RModel.class);
//		schema.saveAsParquetFile("/spark/recommendation/rating/parquet");
//		sc.stop();
	}

}
