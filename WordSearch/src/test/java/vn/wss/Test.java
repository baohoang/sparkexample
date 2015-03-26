package vn.wss;

import static org.junit.Assert.*;
import vn.wss.wordsearch.StringUtils;

public class Test {

	@org.junit.Test
	public void test() {
		String s ="http://websosanh.vn/s/cat-082/iphone+6+plus/filter-r_79.htm";
		System.out.println(StringUtils.getWordSearch(s));
	}

}
