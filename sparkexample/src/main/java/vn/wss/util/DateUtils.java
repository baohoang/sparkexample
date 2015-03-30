package vn.wss.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import org.joda.time.DateTime;

public class DateUtils {
	public static Date getYesterday() {
		Date date = new Date();
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.add(Calendar.DATE, -1);
		return c.getTime();
	}

	public static Date addDays(Date date, int days) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.add(Calendar.DATE, days);
		return c.getTime();
	}

	public static Date getStartOfDay(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH);
		int day = calendar.get(Calendar.DATE);
		calendar.set(year, month, day, 0, 0, 0);
		calendar.add(Calendar.MILLISECOND, -1);
		return calendar.getTime();
	}

	public static Date getEndOfDay(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH);
		int day = calendar.get(Calendar.DATE);
		calendar.set(year, month, day, 23, 59, 59);
		calendar.set(Calendar.MILLISECOND, 999);
		calendar.add(Calendar.MILLISECOND, 1);
		return calendar.getTime();
	}

	public static Date getFirstPointOfMonth(int year, int month) {
		Calendar calendar = Calendar.getInstance();
		calendar.set(year, month, 1, 0, 0, 0);
		calendar.add(Calendar.DATE, -1);
		Date date = calendar.getTime();
		return date;
	}

	public static Date getEndPointOfMonth(int year, int month) {
		Calendar calendar = Calendar.getInstance();
		calendar.set(year, month + 1, 1, 0, 0, 0);
		Date date = calendar.getTime();
		return date;
	}

	public static Date readTimeStamp(String arg) throws IOException {
		File f = new File(arg);
		BufferedReader br = new BufferedReader(new FileReader(f));
		String s = br.readLine();
		DateTime dateTime = new DateTime(Long.valueOf(s));
		br.close();
		return dateTime.toDate();
	}

	public static void saveTimeStamp(Date from, String arg) throws IOException {
		File f = new File(arg);
		BufferedWriter bwr = new BufferedWriter(new FileWriter(f));
		bwr.write(String.valueOf(from.getTime()));
		bwr.close();
	}
}