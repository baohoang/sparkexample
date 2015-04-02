package vn.wss.spark.recommendation;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import scala.Tuple2;
import vn.wss.spark.model.PModel;
import vn.wss.spark.model.SimilarModel;
import vn.wss.spark.model.TModel;
import vn.wss.spark.model.TrackingModel;
import vn.wss.spark.model.Visitors;
import vn.wss.util.DateUtils;

import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;

public class Recommendation {
	public static void main(String[] args) throws IOException {
		Date from = DateUtils.readTimeStamp(args[0]);
		Date now = new Date();
		String fromStr = DateUtils.dateToString(from);
		String nowStr = DateUtils.dateToString(now);
		DateUtils.saveTimeStamp(now, args[0]);
		int yearMonth = DateUtils.getYearMonth(now);
		SparkConf conf = new SparkConf(true).set(
				"spark.cassandra.connection.host", "10.0.0.11");

		JavaSparkContext sc = new JavaSparkContext(conf);
		CassandraJavaRDD<TrackingModel> rawData = javaFunctions(sc)
				.cassandraTable("tracking", "tracking",
						mapRowTo(TrackingModel.class))
				.select("uri", "user_id")
				.where("year_month = ? AND at > ? and at < ?", yearMonth,
						fromStr, nowStr);
		JavaRDD<TrackingModel> raw = rawData
				.filter(new Function<TrackingModel, Boolean>() {

					@Override
					public Boolean call(TrackingModel v1) throws Exception {
						// TODO Auto-generated method stub
						return v1.getUser_id() != null && v1.getUri() != null
								&& v1.getUri().endsWith("so-sanh.htm");
					}
				});
		JavaRDD<PModel> input = raw.map(new Function<TrackingModel, PModel>() {

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
		}).distinct();// xu li input
		SQLContext sqlContext = new SQLContext(sc);
		DataFrame rawFrame = sqlContext.load("/spark/rawdata/parquet");
		DataFrame similarFrame = sqlContext.load("/spark/similars/parquet");
		DataFrame visitorsFrame = sqlContext.load("/spark/visitors/parquet");
		DataFrame itemsFrame = sqlContext.load("/spark/typeitems/parquet");
		DataFrame usersFrame = sqlContext.load("/spark/typeusers/parquet");
		DataFrame ratingsFrame = sqlContext.load("/spark/ratings/parquet");
		DataFrame resultFrame = sqlContext.load("/spark/result/parquet");
		DataFrame inputFrame = sqlContext.createDataFrame(input, PModel.class);

		// get subtract
		inputFrame = inputFrame.except(rawFrame);

		// update rawData
		rawFrame = rawFrame.unionAll(inputFrame);
		rawFrame.save("/spark/rawdata/parquet", "parquet",
				SaveMode.Overwrite);

		// get resources
		JavaPairRDD<Long, String> users = usersFrame.toJavaRDD().mapToPair(
				new PairFunction<Row, Long, String>() {

					@Override
					public Tuple2<Long, String> call(Row t) throws Exception {
						// TODO Auto-generated method stub
						long id = t.getLong(0);
						String list = t.getString(1);
						return new Tuple2<Long, String>(id, list);
					}
				});

		// get input
		JavaPairRDD<Long, String> uis = inputFrame.toJavaRDD().mapToPair(
				new PairFunction<Row, Long, String>() {

					@Override
					public Tuple2<Long, String> call(Row t) throws Exception {
						// TODO Auto-generated method stub
						long iditem = t.getLong(0);
						long iduser = t.getLong(1);
						return new Tuple2<Long, String>(iduser, iditem + ",");
					}
				});

		// update users
		JavaRDD<TModel> userFinal = users.union(uis)
				.reduceByKey(new Function2<String, String, String>() {

					@Override
					public String call(String v1, String v2) throws Exception {
						// TODO Auto-generated method stub
						return v1 + v2;
					}
				}).map(new Function<Tuple2<Long, String>, TModel>() {

					@Override
					public TModel call(Tuple2<Long, String> v1)
							throws Exception {
						// TODO Auto-generated method stub
						return new TModel(v1._1(), v1._2());
					}
				});
		usersFrame = sqlContext.createDataFrame(userFinal, TModel.class);
		usersFrame.save("/spark/typeusers/parquet", "parquet",
				SaveMode.Overwrite);

		// update ratings
		inputFrame.javaRDD().mapToPair(new PairFunction<Row, Long, Long>() {

			@Override
			public Tuple2<Long, Long> call(Row t) throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
		}).rightOuterJoin(null);
		
		sc.stop();
	}
}