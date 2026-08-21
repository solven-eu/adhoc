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
package eu.solven.adhoc.model.measure;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

/**
 * The tags carrying a meaning beyond a cube author's own classification.
 *
 * <p>
 * Most are stamped by the engine, so a reader encountering one in {@link IHasTags#getTags()} has no way to guess what
 * it means — hence one place naming them all. A tag not listed here is free-form: Adhoc attaches no behaviour to it.
 * </p>
 *
 * <p>
 * Clients are expected to act on some of these. Pivotable preselects {@link #TAG_ESSENTIAL} on a user's first visit to
 * a cube, and keeps {@link #TAG_HIDDEN} entries out of its wizard.
 * </p>
 *
 * @author Benoit Lacelle
 */
public interface IAdhocTags {

	/**
	 * Measures and columns worth starting from, on a cube carrying too many to browse. Advisory: it expresses that a
	 * measure is useful in most cases, not that it is required.
	 */
	String TAG_ESSENTIAL = "essential";

	/**
	 * Measures and columns which should stay out of the way unless explicitly asked for.
	 */
	String TAG_HIDDEN = "hidden";

	/**
	 * A measure whose evaluation is traced, independently of the query carrying {@code StandardQueryOptions.DEBUG}.
	 */
	String TAG_DEBUG = "debug";

	/**
	 * A column computed by the cube rather than read from the underlying table.
	 */
	String TAG_CALCULATED = "calculated";

	/**
	 * A column whose coordinates are produced by an {@code IColumnGenerator} rather than stored in the table.
	 */
	String TAG_GENERATED = "generated";

	/**
	 * A column describing the cube itself, such as which sub-cube a row came from.
	 */
	String TAG_META = "meta";

	/**
	 * A column present in every sub-cube of a composite cube, as opposed to only some of them.
	 */
	String TAG_COMPOSITE_FULL = "composite-full";

	/**
	 * Engine plumbing, carrying no business meaning.
	 */
	String TAG_TECHNICAL = "technical";

	/**
	 * A human-readable note per baked-in tag, so a client can explain a tag it did not choose without hard-coding a
	 * copy of this vocabulary.
	 *
	 * <p>
	 * Kept next to the constants deliberately: a description living apart from the tag it documents drifts from it. A
	 * project describes its own tags through {@code IAdhocSchemaRegistrer#describeTag(String, String)}, which also
	 * allows overriding any entry here.
	 * </p>
	 *
	 * <p>
	 * The wording states what a tag <em>means</em>, not what a given client does about it: what Pivotable does with
	 * {@link #TAG_ESSENTIAL} is Pivotable's to describe, and another client may reasonably act differently.
	 * </p>
	 */
	Map<String, String> TAG_DESCRIPTIONS = ImmutableMap.<String, String>builder()
			.put(TAG_ESSENTIAL,
					"Useful in most cases. A good starting point on a cube carrying too many entries to browse.")
			.put(TAG_HIDDEN, "Should stay out of the way unless explicitly asked for.")
			.put(TAG_DEBUG,
					"Evaluation is traced by the engine, whether or not the query itself asks for debug output.")
			.put(TAG_CALCULATED, "Computed by the cube rather than read from the underlying table.")
			.put(TAG_GENERATED, "Coordinates are produced by a column generator rather than stored in the table.")
			.put(TAG_META, "Describes the cube itself, such as which sub-cube a row came from.")
			.put(TAG_COMPOSITE_FULL, "Present in every sub-cube of a composite cube, as opposed to only some of them.")
			.put(TAG_TECHNICAL, "Engine plumbing, carrying no business meaning.")
			.build();
}
