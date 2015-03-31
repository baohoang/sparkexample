package vn.wss.spark.recommendation;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import vn.wss.spark.model.PModel;

public class Test {
	public static void main(String[] args) throws IOException {
		SparkConf conf = new SparkConf(true);
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		JavaRDD<String> log = sc.textFile("/log.txt",1);
		Configuration configuration = new Configuration();
		FileSystem hdfs = FileSystem.get(URI.create("hdfs://master:9000"),
				configuration);
		if (hdfs.exists(new Path("/spark/rawdata/parquet"))) {
			hdfs.delete(new Path("/spark/rawdata/parquet"), true);
		}
		JavaRDD<PModel> rs = log.map(new Function<String, PModel>() {

			@Override
			public PModel call(String v1) throws Exception {
				// TODO Auto-generated method stub
				String[] s = v1.split(",");
				long user = Long.parseLong(s[0]);
				long item = Long.parseLong(s[1]);
				return new PModel(user, item);
			}
		}).distinct();
		DataFrame data = sqlContext.createDataFrame(rs, PModel.class);
		data.saveAsParquetFile("/spark/rawdata/parquet");
		sc.stop();
	}
}
