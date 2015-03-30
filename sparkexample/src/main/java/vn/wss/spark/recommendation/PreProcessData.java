package vn.wss.spark.recommendation;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapColumnTo;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import vn.wss.spark.model.PModel;
import vn.wss.spark.model.TrackingModel;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

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
		logger.info(rawData.count());
		JavaRDD<PModel> data = rawData.filter(
				new Function<TrackingModel, Boolean>() {

					@Override
					public Boolean call(TrackingModel v1) throws Exception {
						// TODO Auto-generated method stub
						return v1.getUser_id() != null
								&& v1.getUri().endsWith("so-sanh.htm");
					}
				}).map(new Function<TrackingModel, PModel>() {

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
		SQLContext sqlContext = new SQLContext(sc);
		DataFrame schemaPeople = sqlContext.createDataFrame(data,
				PModel.class);
		schemaPeople.registerTempTable("pairmodel");
		logger.info("create table completed ...");
		// Calendar calendar = Calendar.getInstance();
		// calendar.set(2015, 1, 1, 0, 0, 0);
		// Date date = calendar.getTime();
		// Date now = new Date();
		// while (!date.after(now)) {
		// // Date d1 = DateUtils.getStartOfDay(date);
		// // Date d2 = DateUtils.getEndOfDay(date);
		// Calendar c = Calendar.getInstance();
		// c.setTime(date);
		// int year = c.get(Calendar.YEAR);
		// int month = c.get(Calendar.MONTH);
		// int day = c.get(Calendar.DAY_OF_MONTH);
		// int year_month = year * 100 + month;
		// String path = "/spark/" + year + "/" + month + "/" + day;
		// String d1 = year + "-" + month + "-" + day + " " + "0:0:0+0700";
		// String d2 = year + "-" + month + "-" + (day + 1) + " "
		// + "0:0:0+0700";
		// JavaPairRDD<LongWritable, LongWritable> data = rawData
		// .where("year_month = ? AND at > ? AND at < ?", year_month,
		// d1, d2)
		// .mapToPair(
		// new PairFunction<CassandraRow, LongWritable, LongWritable>() {
		//
		// @Override
		// public Tuple2<LongWritable, LongWritable> call(
		// CassandraRow t) throws Exception {
		// // TODO Auto-generated method stub
		// String userIdStr = t.getString("user_id");
		// String uri = t.getString("uri");
		// String itemIdStr = StringUtils
		// .getItemIDStr(uri);
		// if (userIdStr != null && itemIdStr != null) {
		// LongWritable userid = new LongWritable(
		// Long.parseLong(userIdStr));
		// LongWritable itemid = new LongWritable(
		// Long.parseLong(itemIdStr));
		// return new Tuple2<LongWritable, LongWritable>(
		// userid, itemid);
		// }
		// return null;
		// }
		// });
		// // logger.info(date.toString() + " has completed with " +
		// // data.count());
		//
		// data.saveAsHadoopFile(path, LongWritable.class, LongWritable.class,
		// SequenceFileOutputFormat.class);
		// date = DateUtils.addDays(date, 1);
		// }
		sc.stop();
	}
}
