package eu.solven.adhoc.api.v1.pojo;

import java.util.regex.Pattern;

import lombok.Builder;
import lombok.Getter;
import lombok.Value;

/**
 * To be used with {@link ColumnFilter}, for regex-based matchers.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
public class LikeMatcher implements IValueMatcher {
	String like;

	public static boolean like(final String likePattern, final CharSequence inputToTest) {
		Pattern p = asPattern(likePattern);
		return p.matcher(inputToTest).matches();
	}

	// https://www.alibabacloud.com/blog/how-to-efficiently-implement-sql-like-syntax-in-java_600079
	// Is it missing % escaping?
	public static Pattern asPattern(final String likePattern) {
		String regexPattern = likePattern.replace("_", ".").replace("%", ".*?");
		return Pattern.compile(regexPattern, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
	}

	@Override
	public boolean match(Object value) {
		// Are we fine turning `null` into `"null"`?
		CharSequence asCharSequence;
		if (value instanceof CharSequence cs) {
			asCharSequence = cs;
		} else {
			// BEWARE Should we require explicit cast, than casting ourselves?
			asCharSequence = String.valueOf(value);
		}
		return LikeMatcher.like(getLike(), asCharSequence);
	}
}
