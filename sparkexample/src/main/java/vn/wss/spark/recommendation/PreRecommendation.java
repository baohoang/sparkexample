package vn.wss.spark.recommendation;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.types.DataType;

import scala.Tuple2;
import scala.collection.Seq;
import vn.wss.spark.model.RModel;
import vn.wss.spark.model.SModel;

public class PreRecommendation {
	private static final Logger logger = LogManager
			.getLogger(PreRecommendation.class);

	public static void main(String[] args) throws IOException {
		SparkConf conf = new SparkConf(true);
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		DataFrame visitorsFrame = sqlContext.load("/spark/visitors/parquet");
		DataFrame similarsFrame = sqlContext.load("/spark/similars/parquet");
		JavaPairRDD<Long, Tuple2<Long, Integer>> addC = similarsFrame
				.toJavaRDD().mapToPair(
						new PairFunction<Row, Long, Tuple2<Long, Integer>>() {

							@Override
							public Tuple2<Long, Tuple2<Long, Integer>> call(
									Row v1) throws Exception {
								// TODO Auto-generated method stub
//								logger.info(v1.toString());
								return new Tuple2<Long, Tuple2<Long, Integer>>(
										v1.getLong(0),
										new Tuple2<Long, Integer>(
												v1.getLong(1), v1.getInt(2)));
							}
						});
		JavaPairRDD<Long, Integer> addA = visitorsFrame.toJavaRDD().mapToPair(
				new PairFunction<Row, Long, Integer>() {

					@Override
					public Tuple2<Long, Integer> call(Row v1) throws Exception {
						// TODO Auto-generated method stub
//						logger.info(v1.toString());
						return new Tuple2<Long, Integer>(v1.getLong(0), v1
								.getInt(1));
					}
				});
		JavaRDD<RModel> rate = addC
				.join(addA)
				.mapToPair(
						new PairFunction<Tuple2<Long, Tuple2<Tuple2<Long, Integer>, Integer>>, Long, RModel>() {

							@Override
							public Tuple2<Long, RModel> call(
									Tuple2<Long, Tuple2<Tuple2<Long, Integer>, Integer>> v)
									throws Exception {
								// TODO Auto-generated method stub
								long itemId = v._1();
								long similarId = v._2()._1()._1();
								int a = v._2()._2();
								int c = v._2()._1()._2();
								RModel model = new RModel(itemId, similarId, a,
										-1, c);
								return new Tuple2<Long, RModel>(similarId,
										model);
							}
						})
				.join(addA)
				.map(new Function<Tuple2<Long, Tuple2<RModel, Integer>>, RModel>() {

					@Override
					public RModel call(Tuple2<Long, Tuple2<RModel, Integer>> v1)
							throws Exception {
						// TODO Auto-generated method stub
						RModel model = v1._2()._1();
						model.setB(v1._2()._2());
						return model;
					}
				});
		DataFrame res = sqlContext.createDataFrame(rate, RModel.class);
		Configuration configuration = new Configuration();
		FileSystem hdfs = FileSystem.get(URI.create("hdfs://master:9000"),
				configuration);
		if (hdfs.exists(new Path("/spark/ratings/parquet"))) {
			hdfs.delete(new Path("/spark/ratings/parquet"), true);
		}
		res.saveAsParquetFile("/spark/ratings/parquet");
		sc.stop();
	}
}
