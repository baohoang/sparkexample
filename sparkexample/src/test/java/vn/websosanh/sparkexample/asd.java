package vn.websosanh.sparkexample;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.types.IntegerType;
import org.joda.time.DateTime;
import org.junit.Test;

import vn.wss.util.DateUtils;

public class asd {

	@Test
	public void test() {
		String list="";
		String[] s=list.split(",");
		System.out.println(s.length);
	}

}
