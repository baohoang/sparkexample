package vn.websosanh.sparkexample;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import junit.framework.TestCase;

public class Test1 extends TestCase {
	public void TestDateTimeJoda(){
		DateTime dateTime=new DateTime(DateTimeZone.forOffsetHours(+7));
	}
}
