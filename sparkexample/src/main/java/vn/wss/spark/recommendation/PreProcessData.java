package vn.wss.spark.recommendation;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;
import vn.wss.spark.model.PModel;
import vn.wss.spark.model.TrackingModel;

import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;

public class PreProcessData {
	private static final Logger logger = LogManager
			.getLogger(CassandraConnection.class);

	public static void main(String[] args) {
		SparkConf conf = new SparkConf(true).set(
				"spark.cassandra.connection.host", "10.0.0.11");

		JavaSparkContext sc = new JavaSparkContext(conf);
		CassandraJavaRDD<TrackingModel> rawData = javaFunctions(sc)
				.cassandraTable("tracking", "tracking",
						mapRowTo(TrackingModel.class)).select("uri", "user_id");
		JavaRDD<TrackingModel> raw = rawData
				.filter(new Function<TrackingModel, Boolean>() {

					@Override
					public Boolean call(TrackingModel v1) throws Exception {
						// TODO Auto-generated method stub
						return v1.getUser_id() != null && v1.getUri() != null
								&& v1.getUri().endsWith("so-sanh.htm");
					}
				});
		JavaRDD<PModel> data = raw.map(new Function<TrackingModel, PModel>() {

			@Override
			public PModel call(TrackingModel v1) throws Exception {
				// TODO Auto-generated method stub
				long userid = Long.parseLong(v1.getUser_id());
				String regex = ".*\\/([0-9]+)\\/so-sanh.htm";
				Pattern pattern = Pattern.compile(regex);
				Matcher matcher = pattern.matcher(v1.getUri());
				String itemIDStr = "-1";
				if (matcher.matches()) {
					itemIDStr = matcher.group(1);
				}
				long itemid = Long.parseLong(itemIDStr);
				return new PModel(userid, itemid);
			}
		});
		JavaPairRDD<Long, Long> data2 = raw
				.mapToPair(new PairFunction<TrackingModel, Long, Long>() {

					@Override
					public Tuple2<Long, Long> call(TrackingModel v1)
							throws Exception {
						// TODO Auto-generated method stub
						long userid = Long.parseLong(v1.getUser_id());
						String regex = ".*\\/([0-9]+)\\/so-sanh.htm";
						Pattern pattern = Pattern.compile(regex);
						Matcher matcher = pattern.matcher(v1.getUri());
						String itemIDStr = "-1";
						if (matcher.matches()) {
							itemIDStr = matcher.group(1);
						}
						long itemid = Long.parseLong(itemIDStr);
						return new Tuple2<Long, Long>(userid, itemid);
					}
				}).di;
		SQLContext sqlContext = new SQLContext(sc);
		DataFrame schemaPeople = sqlContext.createDataFrame(data, PModel.class);
		logger.info("create table completed ...");
		schemaPeople.saveAsParquetFile("/spark/rawdata/parquet");
		sc.stop();
	}
}
