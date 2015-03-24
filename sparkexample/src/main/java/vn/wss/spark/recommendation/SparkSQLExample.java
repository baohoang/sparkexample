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
import vn.wss.spark.model.UserForItem;

public class SparkSQLExample {
	private static final String FILE_PATH = "/spark";
	private static final String USER_ITEM = FILE_PATH + "/user4item";

	private static final Logger logger = LogManager
			.getLogger(SparkSQLExample.class);

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		JavaPairRDD<LongWritable, TupleWritable> rawData = sc.sequenceFile(
				USER_ITEM, LongWritable.class, TupleWritable.class);
		JavaRDD<UserForItem> user4item = rawData
				.map(new Function<Tuple2<LongWritable, TupleWritable>, UserForItem>() {

					@Override
					public UserForItem call(
							Tuple2<LongWritable, TupleWritable> t)
							throws Exception {
						// TODO Auto-generated method stub
						long key = t._1().get();
						TupleWritable t2 = t._2();
						ArrayList<Long> list = new ArrayList<>();
						Iterator<Writable> it = t2.iterator();
						while (it.hasNext()) {
							LongWritable v = (LongWritable) it.next();
							list.add(v.get());
						}
						return new UserForItem(key, list);
					}
				});
		DataFrame dataFrame = sqlContext.createDataFrame(user4item,
				UserForItem.class);
		dataFrame.registerTempTable("user4item");
		logger.info("columns name: " + dataFrame.columns().toString());
		logger.info("size before: " + user4item.count());
		JavaPairRDD<Long, List<Long>> load = sqlContext
				.sql("SELECT * FROM user4item").javaRDD()
				.mapToPair(new PairFunction<Row, Long, List<Long>>() {

					@Override
					public Tuple2<Long, List<Long>> call(Row t)
							throws Exception {
						// TODO Auto-generated method stub
						long key = t.getLong(0);
						List<Long> val = t.getList(1);
						return new Tuple2<Long, List<Long>>(key, val);
					}
				});
		logger.info("size after: " + load.count());
	}
}
