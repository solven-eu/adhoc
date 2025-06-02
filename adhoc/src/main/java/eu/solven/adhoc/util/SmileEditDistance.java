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
package eu.solven.adhoc.util;

/**
 * Mostly duplicated from Smile, not to require a full external dependency.
 *
 * @see https://github.com/haifengl/smile/blob/master/base/src/main/java/smile/math/distance/EditDistance.java#L505
 */
class SmileEditDistance {

	protected SmileEditDistance() {
		// hidden
	}

	/**
	 * Levenshtein distance between two strings allows insertion, deletion, or substitution of characters. O(mn) time
	 * and O(n) space. Multi-thread safe.
	 *
	 * @param x
	 *            a string.
	 * @param y
	 *            a string.
	 * @return the distance.
	 */
	@SuppressWarnings("PMD.AvoidArrayLoops")
	public static int levenshtein(String x, String y) {
		// switch parameters to use the shorter one as y to save space.
		if (x.length() < y.length()) {
			String swap = x;
			x = y;
			y = swap;
		}

		int[][] d = new int[2][y.length() + 1];

		for (int j = 0; j <= y.length(); j++) {
			d[0][j] = j;
		}

		for (int i = 1; i <= x.length(); i++) {
			d[1][0] = i;

			for (int j = 1; j <= y.length(); j++) {
				int cost;
				if (x.charAt(i - 1) == y.charAt(j - 1)) {
					cost = 0;
				} else {
					cost = 1;
				}
				d[1][j] = min(d[0][j] + 1, // deletion
						d[1][j - 1] + 1, // insertion
						d[0][j - 1] + cost); // substitution
			}
			int[] swap = d[0];
			d[0] = d[1];
			d[1] = swap;
		}

		return d[0][y.length()];
	}

	/**
	 * Returns the minimum of 3 integer numbers.
	 * 
	 * @param a
	 *            a number.
	 * @param b
	 *            a number.
	 * @param c
	 *            a number.
	 * @return the minimum.
	 */
	private static int min(int a, int b, int c) {
		return Math.min(Math.min(a, b), c);
	}
}
