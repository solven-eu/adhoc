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
package eu.solven.adhoc.engine.cancel;

import java.time.Duration;
import java.time.OffsetDateTime;

import eu.solven.adhoc.engine.context.IIsCancellable;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Helps cancelling queries.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
@Slf4j
public class CancellationHelpers {
	/**
	 * To be called in a finally block of a cancellable block.
	 * 
	 * It would typically log to report failure or delay cancellation. But it would not throw, as we prefer a slightly
	 * delayed succesful queries than a can
	 * 
	 * @param queryPod
	 */
	public static void afterCancellable(IIsCancellable queryPod) {
		if (queryPod.isCancelled()) {
			OffsetDateTime cancellationDate = queryPod.getCancellationDate();
			OffsetDateTime now = OffsetDateTime.now();

			Duration duration = Duration.between(cancellationDate, now);
			log.warn("A cancelled query leaked a part, which remained active for {}",
					PepperLogHelper.humanDuration(duration.toMillis()));
		}
	}

}
