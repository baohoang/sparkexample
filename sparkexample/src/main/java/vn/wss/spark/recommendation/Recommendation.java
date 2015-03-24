package vn.wss.spark.recommendation;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import vn.wss.spark.model.ArrayLongListWritable;
import vn.wss.spark.model.RModel;
import vn.wss.spark.model.RatingWritable;

public class Recommendation implements Serializable {
	/**
	 * 
	 */
	private static final String FILE_PATH = "/spark";
	private static final String RAW_DATA_FILE = FILE_PATH + "/log.txt";
	private static final String RESULT_PATH = FILE_PATH + "/result";
	private static final String ITEM_USER = FILE_PATH + "/item4user";
	private static final String USER_ITEM = FILE_PATH + "/user4item";
	private static final long serialVersionUID = 1L;
	private transient SparkConf conf;
	private static final Logger logger = LogManager
			.getLogger(Recommendation.class);

	private Recommendation(SparkConf conf) {
		this.conf = conf;
	}

	public void run() throws IOException {
		Configuration configuration = new Configuration();
		configuration.set("fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		configuration.set("fs.file.impl",
				org.apache.hadoop.fs.LocalFileSystem.class.getName());
		FileSystem hdfs = FileSystem.get(
				URI.create("hdfs://spark-slave-2:9000"), configuration);
		if (hdfs.exists(new Path(RESULT_PATH))) {
			hdfs.delete(new Path(RESULT_PATH), true);
		}
		if (hdfs.exists(new Path(ITEM_USER))) {
			hdfs.delete(new Path(ITEM_USER), true);
		}
		if (hdfs.exists(new Path(USER_ITEM))) {
			hdfs.delete(new Path(USER_ITEM), true);
		}
		JavaSparkContext sc = new JavaSparkContext(conf);
		// insert process
		// calculate similar C
		// calculate A,B

		JavaPairRDD<Long, Long> rawData = getData(sc);
		JavaPairRDD<Long, Long> a = calculate(rawData);
		JavaPairRDD<Tuple2<Long, Long>, Long> c = calculateSimilar(rawData);
		JavaPairRDD<Long, Tuple2<Long, Double>> res = fusion(c, a);
		res.mapToPair(
				new PairFunction<Tuple2<Long, Tuple2<Long, Double>>, LongWritable, RatingWritable>() {

					@Override
					public Tuple2<LongWritable, RatingWritable> call(
							Tuple2<Long, Tuple2<Long, Double>> t)
							throws Exception {
						// TODO Auto-generated method stub
						LongWritable key = new LongWritable(t._1());
						RatingWritable value = new RatingWritable(
								new LongWritable(t._2()._1()),
								new DoubleWritable(t._2()._2()));
						return new Tuple2<LongWritable, RatingWritable>(key,
								value);
					}
				}).saveAsHadoopFile(RESULT_PATH, LongWritable.class,
				RatingWritable.class, SequenceFileOutputFormat.class);
		logger.info("save file result");
		// res.saveAsTextFile(RESULT_PATH);
		sc.stop();
	}

	public JavaPairRDD<Long, Tuple2<Long, Double>> fusion(
			JavaPairRDD<Tuple2<Long, Long>, Long> c, JavaPairRDD<Long, Long> a) {

		JavaPairRDD<Long, RModel> s1 = c
				.mapToPair(new PairFunction<Tuple2<Tuple2<Long, Long>, Long>, Long, RModel>() {

					@Override
					public Tuple2<Long, RModel> call(
							Tuple2<Tuple2<Long, Long>, Long> t)
							throws Exception {
						// TODO Auto-generated method stub
						RModel model1 = new RModel(t._1()._1(), t._1()._2(),
								-1, -1, t._2());
						long key1 = t._1()._1();
						return new Tuple2<Long, RModel>(key1, model1);
					}
				});
		JavaPairRDD<Long, RModel> s2 = s1
				.join(a)
				.mapToPair(
						new PairFunction<Tuple2<Long, Tuple2<RModel, Long>>, Long, RModel>() {

							@Override
							public Tuple2<Long, RModel> call(
									Tuple2<Long, Tuple2<RModel, Long>> t)
									throws Exception {
								// TODO Auto-generated method stub
								RModel m = t._2()._1();
								long key = m.getSimilarId();
								long a = t._2()._2();
								m.setA(a);
								return new Tuple2<Long, RModel>(key, m);
							}
						});
		JavaRDD<RModel> s3 = s2.join(a).map(
				new Function<Tuple2<Long, Tuple2<RModel, Long>>, RModel>() {

					@Override
					public RModel call(Tuple2<Long, Tuple2<RModel, Long>> v1)
							throws Exception {
						// TODO Auto-generated method stub
						RModel model = v1._2()._1();
						long b = v1._2()._2();
						model.setB(b);
						return model;
					}
				});
		JavaPairRDD<Long, Tuple2<Long, Double>> res = s3
				.mapToPair(new PairFunction<RModel, Long, Tuple2<Long, Double>>() {

					@Override
					public Tuple2<Long, Tuple2<Long, Double>> call(RModel t)
							throws Exception {
						// TODO Auto-generated method stub
						long key = t.getItemId();
						long key2 = t.getSimilarId();
						long a = t.getA();
						long b = t.getB();
						long c = t.getC();
						// logger.info(key + " " + key2 + " " + a + " " + b +
						// " "
						// + c);
						double rating = c / (a + b - c);
						return new Tuple2<Long, Tuple2<Long, Double>>(key,
								new Tuple2<Long, Double>(key2, rating));
					}
				});
		res = res
				.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Tuple2<Long, Double>>, Long, Tuple2<Long, Double>>() {

					@Override
					public Iterable<Tuple2<Long, Tuple2<Long, Double>>> call(
							Tuple2<Long, Tuple2<Long, Double>> t)
							throws Exception {
						// TODO Auto-generated method stub
						List<Tuple2<Long, Tuple2<Long, Double>>> list = new ArrayList<Tuple2<Long, Tuple2<Long, Double>>>();
						list.add(t);
						list.add(new Tuple2<Long, Tuple2<Long, Double>>(t._2()
								._1(), new Tuple2<Long, Double>(t._1(), t._2()
								._2())));
						return list;
					}
				});
		return res;
	}

	public JavaPairRDD<Long, Long> calculate(JavaPairRDD<Long, Long> rawData) {
		JavaPairRDD<Long, Long> userList = rawData
				.mapToPair(new PairFunction<Tuple2<Long, Long>, Long, Long>() {

					@Override
					public Tuple2<Long, Long> call(Tuple2<Long, Long> t)
							throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<Long, Long>(t._2(), t._1());
					}
				});
		JavaPairRDD<Long, Iterable<Long>> userListForItem = userList
				.groupByKey();
		// save SequenceFIle Hadoop
		userListForItem
				.mapToPair(
						new PairFunction<Tuple2<Long, Iterable<Long>>, LongWritable, ArrayLongListWritable>() {

							@Override
							public Tuple2<LongWritable, ArrayLongListWritable> call(
									Tuple2<Long, Iterable<Long>> t)
									throws Exception {
								// TODO Auto-generated method stub
								Iterator<Long> t2 = t._2().iterator();
								List<LongWritable> list = new ArrayList<LongWritable>();
								while (t2.hasNext()) {
									list.add(new LongWritable(t2.next()));
								}
								ArrayLongListWritable value = new ArrayLongListWritable(
										new IntWritable(list.size()), list
												.toArray(new LongWritable[list
														.size()]));
								LongWritable key = new LongWritable(t._1());
								return new Tuple2<LongWritable, ArrayLongListWritable>(
										key, value);
							}
						}).saveAsHadoopFile(USER_ITEM, LongWritable.class,
						ArrayLongListWritable.class, SequenceFileOutputFormat.class);
		logger.info("save file user4item");
		JavaPairRDD<Long, Long> countMapper = userListForItem
				.mapToPair(new PairFunction<Tuple2<Long, Iterable<Long>>, Long, Long>() {

					@Override
					public Tuple2<Long, Long> call(
							Tuple2<Long, Iterable<Long>> t) throws Exception {
						// TODO Auto-generated method stub
						Iterator<Long> iterator = t._2().iterator();
						Set<Long> set = new HashSet<Long>();
						while (iterator.hasNext()) {
							set.add(iterator.next());
						}
						return new Tuple2<Long, Long>(t._1(), (long) set.size());
					}
				});
		JavaPairRDD<Long, Long> countReducer = countMapper
				.reduceByKey(new Function2<Long, Long, Long>() {
					@Override
					public Long call(Long v1, Long v2) throws Exception {
						// TODO Auto-generated method stub
						return v1 + v2;
					}
				});
		logger.info("read count data completed: " + countReducer.count());
		return countReducer;
	}

	public JavaPairRDD<Tuple2<Long, Long>, Long> calculateSimilar(
			JavaPairRDD<Long, Long> rawData) {
		JavaPairRDD<Long, Iterable<Long>> itemListForUser = rawData
				.groupByKey();
		// save SequenceFIle Hadoop
		itemListForUser
				.mapToPair(
						new PairFunction<Tuple2<Long, Iterable<Long>>, LongWritable, ArrayLongListWritable>() {

							@Override
							public Tuple2<LongWritable, ArrayLongListWritable> call(
									Tuple2<Long, Iterable<Long>> t)
									throws Exception {
								// TODO Auto-generated method stub
								Iterator<Long> t2 = t._2().iterator();
								List<LongWritable> list = new ArrayList<LongWritable>();
								while (t2.hasNext()) {
									list.add(new LongWritable(t2.next()));
								}
								ArrayLongListWritable value = new ArrayLongListWritable(
										new IntWritable(list.size()), list
												.toArray(new LongWritable[list
														.size()]));
								LongWritable key = new LongWritable(t._1());
								return new Tuple2<LongWritable, ArrayLongListWritable>(
										key, value);
							}
						}).saveAsHadoopFile(ITEM_USER, LongWritable.class,
						ArrayLongListWritable.class, SequenceFileOutputFormat.class);
		logger.info("save file item4user");
		JavaPairRDD<Long, Long> similarList = itemListForUser
				.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<Long>>, Long, Long>() {

					@Override
					public Iterable<Tuple2<Long, Long>> call(
							Tuple2<Long, Iterable<Long>> t) throws Exception {
						// TODO Auto-generated method stub
						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
						Iterator<Long> iterator = t._2().iterator();
						Set<Long> set = new HashSet<Long>();
						while (iterator.hasNext()) {
							set.add(iterator.next());
						}
						Long[] v = set.toArray(new Long[set.size()]);
						for (int i = 0; i < v.length; i++) {
							for (int j = i + 1; j < v.length; j++) {
								if (v[i] != v[j]) {
									if (v[i] < v[j]) {
										list.add(new Tuple2<Long, Long>(v[i],
												v[j]));
									} else {
										list.add(new Tuple2<Long, Long>(v[j],
												v[i]));
									}
								}
							}
						}
						return list;
					}
				});
		JavaPairRDD<Tuple2<Long, Long>, Long> similarMapper = similarList
				.mapToPair(new PairFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Long>() {

					@Override
					public Tuple2<Tuple2<Long, Long>, Long> call(
							Tuple2<Long, Long> t) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<Tuple2<Long, Long>, Long>(t, (long) 1);
					}
				});
		JavaPairRDD<Tuple2<Long, Long>, Long> similarReducer = similarMapper
				.reduceByKey(new Function2<Long, Long, Long>() {

					@Override
					public Long call(Long v1, Long v2) throws Exception {
						// TODO Auto-generated method stub
						return v1 + v2;
					}
				});
		logger.info("read similar data completed: " + similarReducer.count());
		return similarReducer;
	}

	public static void main(String[] args) throws IOException {
		SparkConf conf = new SparkConf();
		conf.setAppName("Recommendation Trailer");
		// conf.setMaster(args[0]);
		Recommendation recommendation = new Recommendation(conf);
		recommendation.run();
	}

	public JavaPairRDD<Long, Long> getData(JavaSparkContext sc) {
		JavaRDD<String> lines = sc.textFile(RAW_DATA_FILE, 1);
		JavaPairRDD<Long, Long> rawData = lines
				.mapToPair(new PairFunction<String, Long, Long>() {

					@Override
					public Tuple2<Long, Long> call(String t) throws Exception {
						// TODO Auto-generated method stub
						String[] res = t.split(",");
						if (res.length != 2) {
							return null;
						}
						Long t1 = Long.parseLong(res[0]);
						Long t2 = Long.parseLong(res[1]);
						Tuple2<Long, Long> tp = new Tuple2<Long, Long>(t1, t2);
						return tp;
					}
				});
		logger.info("read raw data completed: " + rawData.count());
		return rawData;
	}
}
