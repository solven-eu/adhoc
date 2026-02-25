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
package eu.solven.adhoc.util.mermaid.testdata;

import eu.solven.adhoc.util.mermaid.testdata.ISimpleEngine.SimpleEngine;
import lombok.Builder;
import lombok.Builder.Default;

/**
 * Test fixture: root class with two fields.
 *
 * <ul>
 * <li>{@code engine} — has a {@code @Default} value; the graph should expose only the concrete default class.</li>
 * <li>{@code widget} — no default; the graph should expose the interface and all known implementations.</li>
 * </ul>
 */
@Builder
public class SimpleRoot {

	/** Wired to a concrete default: the diagram should show {@link SimpleEngine}, not {@link ISimpleEngine}. */
	@Default
	ISimpleEngine engine = new SimpleEngine();

	/** No default: the diagram should show {@link ISimpleWidget} and its known implementations. */
	ISimpleWidget widget;
}
