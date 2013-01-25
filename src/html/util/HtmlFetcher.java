package html.util;

import java.io.IOException;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class HtmlFetcher {
	public static Document getHtml(String url) {
		Document doc = null;
		try {
			doc = Jsoup
			.connect(url).timeout(10000)
			.ignoreContentType(true) //ignore the content type of this html
			.header("User-Agent",
					"Mozilla/5.0 (Windows; U; Windows NT 5.2) Gecko/2008070208 Firefox/3.0.1")
			.header("Accept", "text/html,application/xhtml+xml")
			.header("Accept-Language", "zh-cn,zh;q=0.5")
			.header("Accept-Charset", "GB2312,utf-8;q=0.7,*;q=0.7")
			.get();
			
		} catch (IOException e) {

			e.printStackTrace();
		}
		return doc;
	}
	
	public static Document getHtml(Connection conn) {
		Document doc = null;
		try {
			doc = conn.timeout(10000)
			.ignoreContentType(true) //ignore the content type of this html
			.header("User-Agent",
					"Mozilla/5.0 (Windows; U; Windows NT 5.2) Gecko/2008070208 Firefox/3.0.1")
			.header("Accept", "text/html,application/xhtml+xml")
			.header("Accept-Language", "zh-cn,zh;q=0.5")
			.header("Accept-Charset", "GB2312,utf-8;q=0.7,*;q=0.7")
			.get();
			
		} catch (IOException e) {

			e.printStackTrace();
		}
		return doc;
	}

}
