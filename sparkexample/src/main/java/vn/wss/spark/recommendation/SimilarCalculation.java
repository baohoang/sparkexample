package vn.wss.spark.recommendation;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;
import vn.wss.spark.model.SimilarModel;
import vn.wss.spark.model.TModel;
import vn.wss.spark.model.TypeModel;

public class SimilarCalculation {
	private static final Logger logger = LogManager
			.getLogger(SimilarCalculation.class);

	public static void main(String[] args) throws IOException {
		SparkConf conf = new SparkConf(true);
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		DataFrame rawFrame = sqlContext.load("/spark/typeusers/parquet");
		JavaRDD<Row> rows = rawFrame.toJavaRDD();
		JavaRDD<SimilarModel> data = rows
				.flatMapToPair(new PairFlatMapFunction<Row, Long, Long>() {

					@Override
					public Iterable<Tuple2<Long, Long>> call(Row t)
							throws Exception {
						// TODO Auto-generated method stub
						List<Tuple2<Long, Long>> res = new ArrayList<Tuple2<Long, Long>>();
						long id = t.getLong(0);
						String listStr = t.getString(1);
						String[] s = listStr.split(",");
						logger.info(id + " " + s.length);
						TypeModel model = new TModel(id, listStr).convert();
						List<Long> list = model.getList();
						for (int i = 0; i < s.length; i++) {
							long a = Long.parseLong(s[i]);
							for (int j = i + 1; j < s.length; j++) {
								long b = Long.parseLong(s[j]);
								if (a > b) {
									res.add(new Tuple2<Long, Long>(b, a));
								} else {
									if (a < b) {
										res.add(new Tuple2<Long, Long>(a, b));
									}
								}
							}
						}
						return res;
					}
				})
				.mapToPair(
						new PairFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Integer>() {

							@Override
							public Tuple2<Tuple2<Long, Long>, Integer> call(
									Tuple2<Long, Long> t) throws Exception {
								// TODO Auto-generated method stub
								return new Tuple2<Tuple2<Long, Long>, Integer>(
										t, 1);
							}
						})
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					@Override
					public Integer call(Integer v1, Integer v2)
							throws Exception {
						// TODO Auto-generated method stub
						return v1 + v2;
					}
				})
				.map(new Function<Tuple2<Tuple2<Long, Long>, Integer>, SimilarModel>() {

					@Override
					public SimilarModel call(
							Tuple2<Tuple2<Long, Long>, Integer> v1)
							throws Exception {
						// TODO Auto-generated method stub
						return new SimilarModel(v1._1()._1(), v1._1()._2(), v1
								._2());
					}
				});
		Configuration configuration = new Configuration();
		FileSystem hdfs = FileSystem.get(URI.create("hdfs://master:9000"),
				configuration);
		if (hdfs.exists(new Path("/spark/similars/parquet"))) {
			hdfs.delete(new Path("/spark/similars/parquet"), true);
		}
		DataFrame res = sqlContext.createDataFrame(data, SimilarModel.class);
		res.saveAsParquetFile("/spark/similars/parquet");
		sc.stop();
	}
}
