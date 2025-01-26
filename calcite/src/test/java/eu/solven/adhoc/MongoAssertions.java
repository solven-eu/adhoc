/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package eu.solven.adhoc;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.regex.Pattern;

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.test.CalciteAssert;

/**
 * Util class which needs to be in the same package as {@link CalciteAssert} due to package-private visibility.
 */
public class MongoAssertions {

	private static final Pattern PATTERN = Pattern.compile("\\.0$");

	private MongoAssertions() {
	}

	/**
	 * Similar to {@link CalciteAssert#checkResultUnordered}, but filters strings before comparing them.
	 *
	 * @param lines
	 *            Expected expressions
	 * @return validation function
	 */
	// public static Consumer<ResultSet> checkResultUnordered(
	// final String... lines) {
	// return resultSet -> {
	// try {
	// final List<String> expectedList =
	// Ordering.natural().immutableSortedCopy(Arrays.asList(lines));
	//
	// final List<String> actualList = new ArrayList<>();
	// CalciteAssert.toStringList(resultSet, actualList);
	// for (int i = 0; i < actualList.size(); i++) {
	// String s = actualList.get(i);
	// s = s.replace(".0;", ";");
	// s = PATTERN.matcher(s).replaceAll("");
	// actualList.set(i, s);
	// }
	// Collections.sort(actualList);
	//
	// assertThat(Ordering.natural().immutableSortedCopy(actualList),
	// equalTo(expectedList));
	// } catch (SQLException e) {
	// throw TestUtil.rethrow(e);
	// }
	// };
	// }

	/**
	 * Whether to run Mongo integration tests. Enabled by default, however test is only included if "it" profile is
	 * activated ({@code -Pit}). To disable, specify {@code -Dcalcite.test.mongodb=false} on the Java command line.
	 *
	 * @return Whether current tests should use an external mongo instance
	 */
	public static boolean useMongo() {
		return CalciteSystemProperty.INTEGRATION_TEST.value() && CalciteSystemProperty.TEST_MONGODB.value();
	}

	/**
	 * Checks wherever tests should use Embedded Fake Mongo instead of connecting to real mongodb instance. Opposite of
	 * {@link #useMongo()}.
	 *
	 * @return Whether current tests should use embedded <a href="https://github.com/bwaldvogel/mongo-java-server">Mongo
	 *         Java Server</a> instance
	 */
	public static boolean useFake() {
		return !useMongo();
	}

	/**
	 * Used to skip tests if current instance is not mongo. Some functionalities are not available in fongo.
	 *
	 * @see <a href="https://github.com/fakemongo/fongo/issues/152">Aggregation with $cond (172)</a>
	 */
	public static void assumeRealMongoInstance() {
		assumeTrue(useMongo(), "Expect mongo instance");
	}
}