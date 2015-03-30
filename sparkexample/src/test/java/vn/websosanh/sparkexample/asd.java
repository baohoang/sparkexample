package vn.websosanh.sparkexample;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;
import org.junit.Test;

import vn.wss.util.DateUtils;

public class asd {

	@Test
	public void test() {
		// fail("Not yet implemented");
		String[] a={"a","a","b"};
		String[] b={"c","b","b"};
		List<String> a1=new ArrayList<String>();
		a1.add("a");
		a1.add("a");
		a1.add("b");
		List<String> a2=new ArrayList<String>();
		a1.add("c");
		a1.add("b");
		a1.add("b");
		SparkConf conf=new SparkConf(true);
		JavaSparkContext sc = new JavaSparkContext();
		JavaRDD<String> c=sc.parallelize(a1);
		JavaRDD<String> c1=sc.parallelize(a2);
		a1=c.union(c1).collect();
		for(String s:a1){
			System.out.println(s);
		}
		
	}

}
