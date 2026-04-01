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
package eu.solven.adhoc.table.arrow;

import java.util.List;

/**
 * Mutable cursor state for an Arrow record-batch reader held by {@link ArrowBatchSpliterator}. Grouping these fields
 * makes the cursor boundary explicit and keeps {@link ArrowBatchSpliterator} focused on splitting logic.
 *
 * <p>
 * All fields are accessed either exclusively on the calling thread (sequential mode) or from sequentially-chained VT
 * tasks (parallel mode via {@link ArrowBatchSpliterator#takeBatch()}), so no additional synchronisation is needed
 * beyond the {@code volatile} on {@code exhausted}.
 *
 * @author Benoit Lacelle
 */
class ArrowReaderCursor {
	/** The current {@code VectorSchemaRoot} loaded from the Arrow reader. */
	Object root;
	/** Field vectors extracted from {@link #root}. */
	List<?> vectors;
	/** Number of rows in the current {@link #root}. */
	int batchSize;
	/** Index of the next row to consume within the current batch. */
	int rowIndex;
	/**
	 * Set to {@code true} when the Arrow reader reports no more batches. {@code volatile} because it is written by VT
	 * tasks (inside sequentially-chained {@link java.util.concurrent.CompletableFuture}s) and read by
	 * {@link ArrowBatchSpliterator#fillPrefetchQueue()} on the calling thread. The {@code volatile} is the lightest
	 * mechanism that guarantees cross-thread visibility without locking.
	 */
	@SuppressWarnings("PMD.AvoidUsingVolatile")
	volatile boolean exhausted;
}
