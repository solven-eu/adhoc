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
package eu.solven.adhoc.util;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method whose execution may block on I/O (network, disk, JDBC, …) for an unbounded duration.
 *
 * <p>
 * Concretely:
 * <ul>
 * <li>The method MUST NOT be called from a {@code ForkJoinPool} worker (notably the common pool used by
 * {@code parallel streams} and {@code CompletableFuture.supplyAsync(...)} without an explicit executor): a blocking
 * call there starves the pool's bounded carrier threads and deadlocks unrelated parallel work that happens to share the
 * same FJP.</li>
 * <li>The method IS a good candidate for a {@code VirtualThread} executor: VTs unmount on blocking calls so a thousand
 * concurrent blocked VTs cost essentially nothing in carrier threads.</li>
 * </ul>
 *
 * <p>
 * This annotation has {@link RetentionPolicy#CLASS} retention so static-analysis tooling (IntelliJ inspections,
 * SpotBugs custom detectors) can flag misuse without paying the runtime overhead of {@code RUNTIME} retention.
 *
 * @author Benoit Lacelle
 */
@Target({ ElementType.METHOD, ElementType.CONSTRUCTOR })
@Retention(RetentionPolicy.CLASS)
public @interface Blocking {
	/**
	 * @return optional human-readable description of what blocks (e.g. {@code "JDBC round-trip"},
	 *         {@code "fans out to sub-cubes"}).
	 */
	String[] value() default {};
}
