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
package eu.solven.adhoc.dataframe.row;

import java.util.List;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.stream.ConsumingStream;
import eu.solven.adhoc.stream.IConsumingStream;
import lombok.Builder;
import lombok.Singular;

/**
 * A composite of {@link ITabularRecordStream}
 * 
 * @author Benoit Lacelle
 */
@Builder
public class CompositeTabularRecordStream implements ITabularRecordStream {
	@Singular
	ImmutableList<ITabularRecordStream> underlyings;

	@Override
	public IConsumingStream<ITabularRecord> records() {
		return ConsumingStream.<ITabularRecord>builder().source(consumer -> {
			underlyings.stream().forEach(s -> s.records().forEach(consumer));
		}).build();
	}

	@Override
	public boolean isDistinctSlices() {
		if (underlyings.isEmpty()) {
			return true;
		} else if (underlyings.size() == 1) {
			return underlyings.getFirst().isDistinctSlices();
		} else {
			// TODO Why necessary false?
			return false;
		}
	}

	@Override
	public void close() {
		closeAll(underlyings);
	}

	// Relates with Guava Closer, but needed as Closed does not handle AutoCloseable
	// https://github.com/google/guava/issues/3068
	@SuppressWarnings("PMD.CloseResource")
	protected static void closeAll(List<ITabularRecordStream> streams) {
		Throwable firstThrown = null;
		for (ITabularRecordStream stream : streams) {
			try {
				stream.close();
			} catch (Exception e) {
				if (firstThrown == null) {
					firstThrown = e;
				} else {
					firstThrown.addSuppressed(e);
				}
			}
		}
		if (firstThrown instanceof RuntimeException re) {
			throw re;
		} else if (firstThrown != null) {
			throw new RuntimeException(firstThrown);
		}
	}
}
