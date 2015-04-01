package vn.wss.spark.recommendation;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import vn.wss.spark.model.PModel;

public class SparkSQLExample {
	private static final Logger logger = LogManager
			.getLogger(SparkSQLExample.class);

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		List<PModel> a=new ArrayList<PModel>();
		a.add(new PModel(0, 1));
		a.add(new PModel(0, 2));
		a.add(new PModel(0, 3));
		a.add(new PModel(0, 4));
		List<PModel> b=new ArrayList<PModel>();
		b.add(new PModel(0, 1));
		b.add(new PModel(0, 2));
		b.add(new PModel(0, 3));
		b.add(new PModel(0, 4));
		JavaRDD<PModel> x1=sc.parallelize(a);
		JavaRDD<PModel> x2=sc.parallelize(b);
		JavaRDD<PModel> s=x1.union(x2);
		DataFrame d=sqlContext.createDataFrame(s, PModel.class);
		d.show();
//		DataFrame data = sqlContext.load("/spark/rawdata/parquet");
//		//get from raw data 
//		data.toJavaRDD().foreach(new VoidFunction<Row>() {
//			
//			@Override
//			public void call(Row t) throws Exception {
//				// TODO Auto-generated method stub
//				logger.info(t.toString());
//			}
//		});
		sc.stop();
	}

}
