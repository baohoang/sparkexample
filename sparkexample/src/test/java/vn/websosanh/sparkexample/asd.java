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
		String t="http://websosanh.vn/gionee-gn800-5-5mp-2gb-2-sim-trang/3120163158968901227/direct.htm";
		String v="http://websosanh.vn/connspeed-as3-45-2mp-4gb-2-sim-trang/906698604949226070/direct.htm";
		
		String[] s = t.split("/");
		String key = s[s.length - 2];
		System.out.println(key);
	}

}
