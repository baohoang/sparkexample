package vn.wss.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtils {
	public static String getItemIDStr(String uri) {
		String regex = ".*\\/([0-9]+)\\/(so-sanh.htm)";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(uri);
		boolean flag = matcher.matches();
		if (flag) {
			return matcher.group(1);
		}
		return null;
	}
}
