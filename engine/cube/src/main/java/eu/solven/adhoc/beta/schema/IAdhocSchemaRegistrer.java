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
package eu.solven.adhoc.beta.schema;

import java.util.Set;

import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.filter.value.IValueMatcher;
import eu.solven.adhoc.measure.forest.IMeasureForest;
import eu.solven.adhoc.table.ITableWrapper;

/**
 * Enables modifying an {@link IAdhocSchema}.
 * 
 * @author Benoit Lacelle
 */
public interface IAdhocSchemaRegistrer {

	IAdhocSchemaRegistrer registerForest(IMeasureForest fromMeasures);

	IAdhocSchemaRegistrer registerCustomMarker(String cube,
			IValueMatcher matchEq,
			CustomMarkerMetadataGenerator customMarkerMetadataGenerator);

	IAdhocSchemaRegistrer registerTable(ITableWrapper table);

	IAdhocSchemaRegistrer registerCube(ICubeWrapper cube);

	CubeWrapper registerCube(String cubeName, String tableName, String forestName);

	CubeWrapper.CubeWrapperBuilder openCubeWrapperBuilder();

	IAdhocSchemaRegistrer tagColumn(ColumnIdentifier columnIdentifier, Set<String> tags);

	IAdhocSchemaRegistrer tagMeasure(MeasureIdentifier measureIdentifier, Set<String> tags);

	/**
	 * Attach tags to a registered cube. Tags are stored at the schema level (the cube itself is not modified) and
	 * surface on the wire under {@link CubeSchemaMetadata#getTags()}. The motivating use case is helping the
	 * {@code /html/endpoints/.../schema} UI offer a cube picker filtered by tag.
	 *
	 * @param cubeName
	 *            name of an already-registered cube
	 * @param tags
	 *            tags to attach; merged with any tags already attached to the cube
	 */
	IAdhocSchemaRegistrer tagCube(String cubeName, Set<String> tags);

	/**
	 * Attaches a human-readable note to a tag, so a client can explain a tag to a user rather than showing a bare word.
	 *
	 * <p>
	 * Descriptions are schema-wide: a tag names the same concept across every cube of a schema. The baked-in tags of
	 * {@link eu.solven.adhoc.model.measure.IAdhocTags} are described out of the box; calling this for one of them
	 * replaces the default, which is the hook for a project preferring its own wording.
	 * </p>
	 *
	 * @param tag
	 *            the tag being described
	 * @param description
	 *            a short note stating what the tag means
	 */
	IAdhocSchemaRegistrer describeTag(String tag, String description);

}
