/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.fsst;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;

/**
 * Enable customizing the FsstTrainer.
 * 
 * @author Benoit Lacelle
 */
@Builder
@Value
public class FsstTrainerConfig {

	public static final FsstTrainerConfig DEFAULTS = FsstTrainerConfig.builder().build();

	// --- Sampling constants ---
	// we construct FSST symbol tables using a random sample of about 16KB (1<<14)
	// https://github.com/cwida/fsst/blob/master/libfsst.hpp#L133
	private static final int FSST_SAMPLE_TARGET = 1 << 14; // 16KB
	private static final int FSST_SAMPLE_MAX_SZ = 2 * FSST_SAMPLE_TARGET;
	private static final int FSST_SAMPLE_LINE = 1 << 9; // 512

	// the target size of the whole sample
	@Default
	int sampleTarget = FSST_SAMPLE_TARGET;

	// the target size of the sample chunk
	@Default
	int sampleLine = FSST_SAMPLE_LINE;

	public int getSampleMaxSize() {
		int maxSize = getSampleTarget() * 2;

		if (maxSize < 0) {
			maxSize = Integer.MAX_VALUE;
		}

		return maxSize;
	}

}
