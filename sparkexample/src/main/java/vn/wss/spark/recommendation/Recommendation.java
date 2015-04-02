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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;

import vn.wss.spark.model.PModel;
import vn.wss.spark.model.TrackingModel;
import vn.wss.util.DateUtils;

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
		similarFrame.registerTempTable("similar");
		DataFrame visitorsFrame = sqlContext.load("/spark/visitors/parquet");
		visitorsFrame.registerTempTable("visitor");
		DataFrame itemsFrame = sqlContext.load("/spark/typeitems/parquet");
		itemsFrame.registerTempTable("items");
		DataFrame usersFrame = sqlContext.load("/spark/typeusers/parquet");
		usersFrame.registerTempTable("users");
		DataFrame ratingsFrame = sqlContext.load("/spark/ratings/parquet");
		ratingsFrame.registerTempTable("ratings");
		DataFrame resultFrame = sqlContext.load("/spark/result/parquet");
		resultFrame.registerTempTable("result");
		JavaRDD<PModel> r1 = rawFrame.toJavaRDD().map(
				new Function<Row, PModel>() {

					@Override
					public PModel call(Row v1) throws Exception {
						// TODO Auto-generated method stub
						long key = v1.getLong(0);
						long val = v1.getLong(1);
						return new PModel(key, val);
					}
				});
		JavaRDD<PModel> subtract = input.subtract(r1);
		subtract.foreach(new VoidFunction<PModel>() {

			@Override
			public void call(PModel t) throws Exception {
				// TODO Auto-generated method stub
				long itemid = t.getItemID();
				long userid = t.getUserID();

			}
		});// lay phan du lieu input moi' hoan toan
		r1 = r1.union(subtract);
		subtract.foreach(new VoidFunction<PModel>() {

			@Override
			public void call(PModel t) throws Exception {
				// TODO Auto-generated method stub
				long userID = t.getUserID();
				long itemID = t.getItemID();
//				sqlContext.sql
				
			}
		});

	}
}