package eu.solven.adhoc.api.v1.pojo;

import java.util.regex.Pattern;

import lombok.Builder;

/**
 * To be used with {@link ColumnFilter}, for regex-based matchers.
 * 
 * @author Benoit Lacelle
 *
 */
@Builder
public class LikeMatcher {
	String like;

	public static boolean like(final String dest, final String likePattern) {
		Pattern p = asPattern(likePattern);
		return p.matcher(dest).matches();
	}

	// https://www.alibabacloud.com/blog/how-to-efficiently-implement-sql-like-syntax-in-java_600079
	// Is it missing % escaping?
	public static Pattern asPattern(final String likePattern) {
		String regexPattern = likePattern.replace("_", ".").replace("%", ".*?");
		return Pattern.compile(regexPattern, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
	}
}
