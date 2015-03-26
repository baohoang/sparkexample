package vn.websosanh.sparkexample;

import static org.junit.Assert.*;

import java.util.Date;

import org.joda.time.DateTime;
import org.junit.Test;

import vn.wss.util.DateUtils;

public class asd {

	@Test
	public void test() {
		// fail("Not yet implemented");
		Date date = new DateTime(2015, 1, 1, 0, 0).toDate();
		Date now = new DateTime().toDate();
		while (date.before(now)) {
			Date d1=DateUtils.getStartOfDay(date);
			Date d2=DateUtils.getEndOfDay(date);
			System.out.println(date+" "+d1+" "+d2);
			date = DateUtils.addDays(date, 1);
			
		}

	}

}
