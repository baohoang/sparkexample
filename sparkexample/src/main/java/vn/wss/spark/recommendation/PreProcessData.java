package vn.wss.spark.recommendation;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import scala.Tuple2;
import vn.wss.util.DateUtils;
import vn.wss.util.StringUtils;

import com.datastax.spark.connector.japi.CassandraRow;

public class PreProcessData {
	private static final Logger logger = LogManager
			.getLogger(CassandraConnection.class);

	public static void main(String[] args) {
		SparkConf conf = new SparkConf(true).set(
				"spark.cassandra.connection.host", "10.0.0.11");

		JavaSparkContext sc = new JavaSparkContext(conf);
		// entire table as an RDD
		// assumes your table test was created as CREATE TABLE test.kv(key text
		// PRIMARY KEY, value int);
		Calendar calendar = Calendar.getInstance();
		calendar.set(2015, 1, 1, 0, 0, 0);
		Date date = calendar.getTime();
		Date now = new Date();
		while (!date.after(now)) {
			Date d1 = DateUtils.getStartOfDay(date);
			Date d2 = DateUtils.getEndOfDay(date);
			Calendar c = Calendar.getInstance();
			c.setTime(date);
			int year = c.get(Calendar.YEAR);
			int month = c.get(Calendar.MONTH);
			int day = c.get(Calendar.DAY_OF_MONTH);
			int year_month = year * 100 + month;
			String path = "/spark/" + year + "/" + month + "/" + day;
			JavaPairRDD<LongWritable, LongWritable> data = javaFunctions(sc)
					.cassandraTable("tracking", "tracking")
					.where("year_month = ? AND at > ? AND at < ?", year_month,
							d1, d2)
					.mapToPair(
							new PairFunction<CassandraRow, LongWritable, LongWritable>() {

								@Override
								public Tuple2<LongWritable, LongWritable> call(
										CassandraRow t) throws Exception {
									// TODO Auto-generated method stub
									String userIdStr = t.getString("user_id");
									String uri = t.getString("uri");
									String itemIdStr = StringUtils
											.getItemIDStr(uri);
									if (userIdStr != null && itemIdStr != null) {
										LongWritable userid = new LongWritable(
												Long.parseLong(userIdStr));
										LongWritable itemid = new LongWritable(
												Long.parseLong(itemIdStr));
										return new Tuple2<LongWritable, LongWritable>(
												userid, itemid);
									}
									return null;
								}
							});
			logger.info(date.toString() + " has completed with " + data.count());

			data.saveAsHadoopFile(path, LongWritable.class, LongWritable.class,
					SequenceFileOutputFormat.class);
			date = DateUtils.addDays(date, 1);
		}
		sc.stop();
		// print some basic stats
	}
}
