package vn.wss.spark.recommendation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;
import vn.wss.spark.model.ArrayLongList;
import vn.wss.spark.model.Rating;
import vn.wss.spark.model.RatingWritable;
import vn.wss.spark.model.UserForItem;

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
		logger.info("reading ...");
		JavaPairRDD<LongWritable, RatingWritable> rawData = sc.sequenceFile(
				RESULT_PATH, LongWritable.class, RatingWritable.class);
		logger.info("read item" + rawData.count());
		JavaRDD<Rating> user4item = rawData
				.map(new Function<Tuple2<LongWritable, RatingWritable>, Rating>() {

					@Override
					public Rating call(Tuple2<LongWritable, RatingWritable> t)
							throws Exception {
						// TODO Auto-generated method stub
						long key = t._1().get();
						RatingWritable t2 = t._2();
						Rating rating = new Rating(t2.getId().get(), t2
								.getRate().get());
						return rating;
					}
				});
		logger.info("size before: " + user4item.count());
		DataFrame dataFrame = sqlContext.createDataFrame(user4item,
				Rating.class);
		logger.info("columns name: " + dataFrame.columns().toString());
		logger.info("creating ...");
		dataFrame.registerTempTable("rating");
		JavaRDD<Rating> load = sqlContext
				.sql("SELECT * FROM rating WHERE id = 204422435").javaRDD()
				.map(new Function<Row, Rating>() {

					@Override
					public Rating call(Row v1) throws Exception {
						// TODO Auto-generated method stub
						long id = v1.getLong(0);
						double r = v1.getDouble(1);
						return new Rating(id, r);
					}
				});
		logger.info("size after: " + load.count());
	}
}
