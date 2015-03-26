package vn.wss;

import static org.junit.Assert.*;
import vn.wss.wordsearch.StringUtils;

public class Test {

	@org.junit.Test
	public void test() {
		String s ="http://websosanh.vn";
		System.out.println(StringUtils.getWordSearch(s));
	}

}
