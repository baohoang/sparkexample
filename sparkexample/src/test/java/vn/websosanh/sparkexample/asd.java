package vn.websosanh.sparkexample;

import static org.junit.Assert.*;

import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.DateTime;
import org.junit.Test;

import vn.wss.util.DateUtils;

public class asd {

	@Test
	public void test() {
		// fail("Not yet implemented");
		String s1="CassandraRow{year_month: 201501, at: 2015-01-31 13:06:45+0700, uri: http://websosanh.vn/s/Tai Nghe Nhạc/trang-3.htm}";
		String s = "CassandraRow{year_month: 201501, at: 2015-01-31 13:06:46+0700, uri: http://websosanh.vn/ban-do-chien-luoc-david-p-norton-robert/1874120356/so-sanh.htm}";
		String regex=".*, uri: http://websosanh.vn/s/(.*).htm}";
		Pattern p=Pattern.compile(regex);
		Matcher m=p.matcher(s1);
		System.out.println(m.matches());
	}

}
