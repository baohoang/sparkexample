package vn.wss.spark.recommendation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import vn.wss.spark.model.RModel;

public class RecommendationSpark {
//	private static final Logger logger = LogManager
//			.getLogger(RecommendationSpark.class);
//
//	public static JavaPairRDD<Long, Long> calculate(
//			JavaPairRDD<Long, Long> rawData) {
//		JavaPairRDD<Long, Iterable<Long>> userListForItem = rawData
//				.groupByKey();
//		JavaPairRDD<Long, Long> countMapper = userListForItem
//				.mapToPair(new PairFunction<Tuple2<Long, Iterable<Long>>, Long, Long>() {
//
//					@Override
//					public Tuple2<Long, Long> call(
//							Tuple2<Long, Iterable<Long>> t) throws Exception {
//						// TODO Auto-generated method stub
//						Iterator<Long> iterator = t._2().iterator();
//						Set<Long> set = new HashSet<Long>();
//						while (iterator.hasNext()) {
//							set.add(iterator.next());
//						}
//						return new Tuple2<Long, Long>(t._1(), (long) set.size());
//					}
//				});
//		JavaPairRDD<Long, Long> countReducer = countMapper
//				.reduceByKey(new Function2<Long, Long, Long>() {
//					@Override
//					public Long call(Long v1, Long v2) throws Exception {
//						// TODO Auto-generated method stub
//						return v1 + v2;
//					}
//				});
//		logger.info("read count data completed: " + countReducer.count());
//		return countReducer;
//	}
//
//	public static JavaPairRDD<Tuple2<Long, Long>, Long> calculateSimilar(
//			JavaPairRDD<Long, Long> rawData) {
//		JavaPairRDD<Long, Long> itemList = rawData
//				.mapToPair(new PairFunction<Tuple2<Long, Long>, Long, Long>() {
//					@Override
//					public Tuple2<Long, Long> call(Tuple2<Long, Long> t)
//							throws Exception {
//						// TODO Auto-generated method stub
//						return new Tuple2<Long, Long>(t._2(), t._1());
//					}
//				});
//		JavaPairRDD<Long, Iterable<Long>> itemListForUser = itemList
//				.groupByKey();
//		JavaPairRDD<Long, Long> similarList = itemListForUser
//				.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<Long>>, Long, Long>() {
//
//					@Override
//					public Iterable<Tuple2<Long, Long>> call(
//							Tuple2<Long, Iterable<Long>> t) throws Exception {
//						// TODO Auto-generated method stub
//						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
//						Iterator<Long> iterator = t._2().iterator();
//						Set<Long> set = new HashSet<Long>();
//						while (iterator.hasNext()) {
//							set.add(iterator.next());
//						}
//						Long[] v = set.toArray(new Long[set.size()]);
//						for (int i = 0; i < v.length; i++) {
//							for (int j = i + 1; j < v.length; j++) {
//								if (v[i] != v[j]) {
//									if (v[i] < v[j]) {
//										list.add(new Tuple2<Long, Long>(v[i],
//												v[j]));
//									} else {
//										list.add(new Tuple2<Long, Long>(v[j],
//												v[i]));
//									}
//								}
//							}
//						}
//						return list;
//					}
//				});
//		JavaPairRDD<Tuple2<Long, Long>, Long> similarMapper = similarList
//				.mapToPair(new PairFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Long>() {
//
//					@Override
//					public Tuple2<Tuple2<Long, Long>, Long> call(
//							Tuple2<Long, Long> t) throws Exception {
//						// TODO Auto-generated method stub
//						return new Tuple2<Tuple2<Long, Long>, Long>(t, (long) 1);
//					}
//				});
//		JavaPairRDD<Tuple2<Long, Long>, Long> similarReducer = similarMapper
//				.reduceByKey(new Function2<Long, Long, Long>() {
//
//					@Override
//					public Long call(Long v1, Long v2) throws Exception {
//						// TODO Auto-generated method stub
//						return v1 + v2;
//					}
//				});
//		logger.info("read similar data completed: " + similarReducer.count());
//		return similarReducer;
//	}
//
//	public static JavaRDD<RModel> fusion(
//			JavaPairRDD<Tuple2<Long, Long>, Long> c, JavaPairRDD<Long, Long> a) {
//
//		JavaPairRDD<Long, RModel> s1 = c
//				.mapToPair(new PairFunction<Tuple2<Tuple2<Long, Long>, Long>, Long, RModel>() {
//
//					@Override
//					public Tuple2<Long, RModel> call(
//							Tuple2<Tuple2<Long, Long>, Long> t)
//							throws Exception {
//						// TODO Auto-generated method stub
//						RModel model1 = new RModel(t._1()._1(), t._1()._2(),
//								-1, -1, t._2());
//						long key1 = t._1()._1();
//						return new Tuple2<Long, RModel>(key1, model1);
//					}
//				});
//		JavaPairRDD<Long, RModel> s2 = s1
//				.join(a)
//				.mapToPair(
//						new PairFunction<Tuple2<Long, Tuple2<RModel, Long>>, Long, RModel>() {
//
//							@Override
//							public Tuple2<Long, RModel> call(
//									Tuple2<Long, Tuple2<RModel, Long>> t)
//									throws Exception {
//								// TODO Auto-generated method stub
//								RModel m = t._2()._1();
//								long key = m.getSimilarId();
//								long a = t._2()._2();
//								m.setA(a);
//								return new Tuple2<Long, RModel>(key, m);
//							}
//						});
//		JavaRDD<RModel> s3 = s2.join(a).map(
//				new Function<Tuple2<Long, Tuple2<RModel, Long>>, RModel>() {
//
//					@Override
//					public RModel call(Tuple2<Long, Tuple2<RModel, Long>> v1)
//							throws Exception {
//						// TODO Auto-generated method stub
//						RModel model = v1._2()._1();
//						long b = v1._2()._2();
//						model.setB(b);
//						return model;
//					}
//				});
//
//		return s3;
//	}
//
//	public static JavaPairRDD<Long, Tuple2<Long, Double>> calRating(
//			JavaRDD<RModel> s3) {
//		JavaPairRDD<Long, Tuple2<Long, Double>> res = s3
//				.mapToPair(new PairFunction<RModel, Long, Tuple2<Long, Double>>() {
//
//					@Override
//					public Tuple2<Long, Tuple2<Long, Double>> call(RModel t)
//							throws Exception {
//						// TODO Auto-generated method stub
//						long key = t.getItemId();
//						long key2 = t.getSimilarId();
//						long a = t.getA();
//						long b = t.getB();
//						long c = t.getC();
//						double rating = c / (a + b - c);
//						return new Tuple2<Long, Tuple2<Long, Double>>(key,
//								new Tuple2<Long, Double>(key2, rating));
//					}
//				});
//		res = res
//				.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Tuple2<Long, Double>>, Long, Tuple2<Long, Double>>() {
//
//					@Override
//					public Iterable<Tuple2<Long, Tuple2<Long, Double>>> call(
//							Tuple2<Long, Tuple2<Long, Double>> t)
//							throws Exception {
//						// TODO Auto-generated method stub
//						List<Tuple2<Long, Tuple2<Long, Double>>> list = new ArrayList<Tuple2<Long, Tuple2<Long, Double>>>();
//						list.add(t);
//						list.add(new Tuple2<Long, Tuple2<Long, Double>>(t._2()
//								._1(), new Tuple2<Long, Double>(t._1(), t._2()
//								._2())));
//						return list;
//					}
//				});
//		return res;
//	}

}
