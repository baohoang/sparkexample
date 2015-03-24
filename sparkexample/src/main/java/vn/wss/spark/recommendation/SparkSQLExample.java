package vn.wss.spark.recommendation;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;
import vn.wss.spark.model.ArrayLongListWritable;
import vn.wss.spark.model.LongList;
import vn.wss.spark.model.Rating;
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
		JavaPairRDD<LongWritable, ArrayLongListWritable> rawData = sc
				.sequenceFile(USER_ITEM, LongWritable.class,
						ArrayLongListWritable.class);
		logger.info("read item" + rawData.count());
		JavaRDD<UserForItem> user4item = rawData
				.map(new Function<Tuple2<LongWritable, ArrayLongListWritable>, UserForItem>() {

					@Override
					public UserForItem call(
							Tuple2<LongWritable, ArrayLongListWritable> t)
							throws Exception {
						// TODO Auto-generated method stub
						long key = t._1().get();
						ArrayLongListWritable t2 = t._2();
						List<Long> list = new ArrayList<Long>();
						for (int i = 0; i < t2.getSize().get(); i++) {
							list.add(t2.getArr()[i].get());
						}
						LongList v = new LongList(list);
						return new UserForItem(key, v);
					}
				});
		logger.info("size before: " + user4item.count());
		DataFrame dataFrame = sqlContext.createDataFrame(user4item,
				UserForItem.class);
		logger.info("columns name: " + dataFrame.columns()[0] + " "
				+ dataFrame.columns()[1]);
		logger.info("creating ...");
		dataFrame.registerTempTable("user4item");
		JavaRDD<UserForItem> load = sqlContext.sql("SELECT * FROM user4item")
				.javaRDD().map(new Function<Row, UserForItem>() {

					@Override
					public UserForItem call(Row v1) throws Exception {
						// TODO Auto-generated method stub
						long id = v1.getLong(0);
						LongList l = new LongList(v1.getList(1));
						return new UserForItem(id, l);
					}
				});
		logger.info("size after: " + load.count());
	}
}
