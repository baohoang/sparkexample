package vn.wss.spark.recommendation;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;
import vn.wss.spark.model.TModel;

public class SaveData {

	private static final Logger logger = LogManager.getLogger(SaveData.class);

	public static void main(String[] args) throws IOException {
		SparkConf conf = new SparkConf(true);
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		DataFrame rawFrame = sqlContext.load("/spark/rawdata/parquet");
		JavaPairRDD<Long, Long> r = rawFrame.toJavaRDD().mapToPair(
				new PairFunction<Row, Long, Long>() {

					@Override
					public Tuple2<Long, Long> call(Row t) throws Exception {
						// TODO Auto-generated method stub
						long key = t.getLong(0);
						long val = t.getLong(1);
						return new Tuple2<Long, Long>(key, val);
					}
				});
		JavaRDD<TModel> r1 = r.groupByKey().map(
				new Function<Tuple2<Long, Iterable<Long>>, TModel>() {

					@Override
					public TModel call(Tuple2<Long, Iterable<Long>> v1)
							throws Exception {
						// TODO Auto-generated method stub
						List<Long> list = new ArrayList<Long>();
						Iterator<Long> iterator = v1._2().iterator();
						while (iterator.hasNext()) {
							list.add(iterator.next());
						}
						return new TModel(v1._1(), list);
					}

				});
		DataFrame schemaR1 = sqlContext.createDataFrame(r1, TModel.class);
		Configuration configuration = new Configuration();
		FileSystem hdfs = FileSystem.get(URI.create("hdfs://master:9000"),
				configuration);
		if (hdfs.exists(new Path("/spark/typeitems/parquet"))) {
			hdfs.delete(new Path("/spark/typeitems/parquet"), true);
		}
		schemaR1.saveAsParquetFile("/spark/typeitems/parquet");
		JavaRDD<TModel> r2 = r
				.mapToPair(new PairFunction<Tuple2<Long, Long>, Long, Long>() {

					@Override
					public Tuple2<Long, Long> call(Tuple2<Long, Long> v1)
							throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<Long, Long>(v1._2(), v1._1());
					}
				}).groupByKey()
				.map(new Function<Tuple2<Long, Iterable<Long>>, TModel>() {
					@Override
					public TModel call(Tuple2<Long, Iterable<Long>> v1)
							throws Exception {
						// TODO Auto-generated method stub
						List<Long> list = new ArrayList<Long>();
						Iterator<Long> iterator = v1._2().iterator();
						while (iterator.hasNext()) {
							list.add(iterator.next());
						}
						return new TModel(v1._1(), list);
					}
				});
		DataFrame schemaR2 = sqlContext.createDataFrame(r2, TModel.class);
		if (hdfs.exists(new Path("/spark/typeusers/parquet"))) {
			hdfs.delete(new Path("/spark/typeusers/parquet"), true);
		}
		schemaR2.saveAsParquetFile("/spark/typeusers/parquet");
		sc.stop();
	}
}
