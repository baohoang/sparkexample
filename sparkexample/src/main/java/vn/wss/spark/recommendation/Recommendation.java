package vn.wss.spark.recommendation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;

public class Recommendation implements Serializable {
	/**
	 * 
	 */
	private static final String filePath = "/spark";
	private static final String rawDataFile = filePath + "/log.txt";
	private static final long serialVersionUID = 1L;
	private transient SparkConf conf;
	private static final Logger logger = LogManager
			.getLogger(Recommendation.class);

	private Recommendation(SparkConf conf) {
		this.conf = conf;
	}

	public void run() {
		JavaSparkContext sc = new JavaSparkContext(conf);
		// insert process
		// calculate similar C
		// calculate A,B
		JavaPairRDD<Long, Long> rawData = getData(sc);
		JavaPairRDD<Long, Long> a = calculate(rawData);
		JavaPairRDD<Tuple2<Long, Long>, Long> c = calculateSimilar(rawData);
		JavaPairRDD<Long, Tuple2<Long, Double>> res = fusion(c, a);
		List<Tuple2<Long, Tuple2<Long, Double>>> list = res.collect();
		for (int i = 0; i < list.size(); i++) {
			logger.info(list.get(i)._1() + " " + list.get(i)._2()._1() + " "
					+ list.get(i)._2()._2());
		}
		sc.stop();
	}

	public JavaPairRDD<Long, Tuple2<Long, Double>> fusion(
			JavaPairRDD<Tuple2<Long, Long>, Long> c, JavaPairRDD<Long, Long> a) {
		JavaPairRDD<Long, Tuple2<Long, Long>> x = c
				.mapToPair(new PairFunction<Tuple2<Tuple2<Long, Long>, Long>, Long, Tuple2<Long, Long>>() {
					@Override
					public Tuple2<Long, Tuple2<Long, Long>> call(
							Tuple2<Tuple2<Long, Long>, Long> t)
							throws Exception {
						// TODO Auto-generated method stub
						Tuple2<Long, Long> t1 = t._1();

						return new Tuple2(t1._1(), new Tuple2<Long, Long>(t1
								._2(), t._2()));
					}
				});
		JavaPairRDD<Long, Tuple2<Long, Long>> y = c
				.mapToPair(new PairFunction<Tuple2<Tuple2<Long, Long>, Long>, Long, Tuple2<Long, Long>>() {

					@Override
					public Tuple2<Long, Tuple2<Long, Long>> call(
							Tuple2<Tuple2<Long, Long>, Long> t)
							throws Exception {
						// TODO Auto-generated method stub
						Tuple2<Long, Long> t1 = t._1();
						return new Tuple2(t1._2(), new Tuple2<Long, Long>(t1
								._1(), t._2()));
					}
				});
		JavaPairRDD<Long, Tuple2<Tuple2<Long, Long>, Long>> f2 = y.join(a);
		JavaPairRDD<Long, Tuple3<Long, Long, Long>> r1 = f2
				.mapToPair(new PairFunction<Tuple2<Long, Tuple2<Tuple2<Long, Long>, Long>>, Long, Tuple3<Long, Long, Long>>() {

					@Override
					public Tuple2<Long, Tuple3<Long, Long, Long>> call(
							Tuple2<Long, Tuple2<Tuple2<Long, Long>, Long>> t)
							throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<Long, Tuple3<Long, Long, Long>>(t
								._2()._1()._1(), new Tuple3<Long, Long, Long>(t
								._1(), t._2()._1()._2(), t._2()._2()));
					}
				});
		JavaPairRDD<Long, Tuple2<Long, Double>> res = a
				.join(r1)
				.mapToPair(
						new PairFunction<Tuple2<Long, Tuple2<Long, Tuple3<Long, Long, Long>>>, Long, Tuple2<Long, Double>>() {

							@Override
							public Tuple2<Long, Tuple2<Long, Double>> call(
									Tuple2<Long, Tuple2<Long, Tuple3<Long, Long, Long>>> t)
									throws Exception {
								// TODO Auto-generated method stub
								Long key = t._1();
								long c = t._2()._2()._2();
								long a = t._2()._1();
								long b = t._2()._2()._3();
								double rating = c / (a + b - c);
								return new Tuple2<Long, Tuple2<Long, Double>>(
										key, new Tuple2<Long, Double>(t._2()
												._2()._1(), rating));
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
		JavaPairRDD<Long, Long> similarList = itemListForUser
				.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<Long>>, Long, Long>() {

					@Override
					public Iterable<Tuple2<Long, Long>> call(
							Tuple2<Long, Iterable<Long>> t) throws Exception {
						// TODO Auto-generated method stub
						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
						Iterable<Long> res = t._2();
						res.iterator().hasNext();
						for (Long i : res) {
							for (Long j : res) {
								if (i != j) {
									list.add(new Tuple2<Long, Long>(i, j));
									list.add(new Tuple2<Long, Long>(j, i));
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

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("Recommendation Trailer");
		// conf.setMaster(args[0]);
		Recommendation recommendation = new Recommendation(conf);
		recommendation.run();
	}

	public JavaPairRDD<Long, Long> getData(JavaSparkContext sc) {
		JavaRDD<String> lines = sc.textFile(rawDataFile, 1);
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
