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
package eu.solven.adhoc.measure.mermaid;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.measure.forest.IMeasureForest;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.model.Filtrator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.model.Partitionor;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.resource.MeasureForests;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Helps to produce Mermaid {@code flowchart} source text describing an {@link IMeasureForest} or a
 * {@link MeasureForests}. The textual output can be pasted at <a href="https://mermaid.live">https://mermaid.live</a>
 * or rendered inline by any Markdown viewer supporting Mermaid — useful for documentation and for previewing a measure
 * graph built programmatically by API.
 *
 * <p>
 * Conceptually a sibling of {@link eu.solven.adhoc.measure.graphviz.ForestAsGraphvizDag}: same per-class shape / color
 * conventions, different target syntax. The Pivotable frontend renders the same conventions live in the interactive
 * measure-graph modal — see {@code pivotable/js/src/main/resources/static/ui/js/adhoc-measures-dag.js}. When you add a
 * new measure class here, update both the Graphviz class and the JS file to keep the three in sync.
 *
 * @author Benoit Lacelle
 */
@Builder
@Slf4j
public class ForestAsMermaidDag {

	// Typical rendered output is a few hundred chars; seed the buffer to avoid the first grow cycles.
	// (PMD's InsufficientStringBufferDeclaration also flags the default-size 16 as too small.)
	private static final int INITIAL_BUFFER_SIZE = 512;

	// IMPORTANT — keep in sync with the GraphViz sibling and the Pivotable frontend:
	// - eu.solven.adhoc.measure.graphviz.ForestAsGraphvizDag#DEFAULT_CLASSTOSHAPE
	// - pivotable/js/src/main/resources/static/ui/js/adhoc-measures-dag.js (shapeAndStyleForType)
	// Shape reference:
	// Partitionor -> `star` -> hexagon {{ ... }}
	// Filtrator -> `invhouse` -> trapezoid-alt [\...\]
	// Dispatchor -> `msquare` -> subroutine [[...]]
	// Aggregator -> `tripleoctagon` -> cylinder [(...)]
	// Combinator -> (no shape) -> rounded ( ... )
	public static final List<Map.Entry<Class<?>, MermaidShape>> DEFAULT_CLASSTOSHAPE =
			ImmutableList.<Map.Entry<Class<?>, MermaidShape>>builder()
					.add(Map.entry(Partitionor.class, new MermaidShape("{{", "}}")))
					.add(Map.entry(Filtrator.class, new MermaidShape("[\\", "\\]")))
					.add(Map.entry(Dispatchor.class, new MermaidShape("[[", "]]")))
					.add(Map.entry(Aggregator.class, new MermaidShape("[(", ")]")))
					.add(Map.entry(Combinator.class, new MermaidShape("(", ")")))
					.build();

	// Hex colors mirroring the `fill` values in adhoc-measures-dag.js (which in turn mirror the
	// named GraphViz colors in ForestAsGraphvizDag#DEFAULT_CLASSTOCOLOR):
	// Partitionor = yellow = #ffef8a
	// Filtrator = darkseagreen = #a6c9a2
	// Combinator = cyan = #a6e3f5
	// Dispatchor = grey = #c8c8c8
	// Aggregator = coral = #ff9b86
	public static final List<Map.Entry<Class<?>, String>> DEFAULT_CLASSTOCOLOR =
			ImmutableList.<Map.Entry<Class<?>, String>>builder()
					.add(Map.entry(Partitionor.class, "#ffef8a"))
					.add(Map.entry(Filtrator.class, "#a6c9a2"))
					.add(Map.entry(Combinator.class, "#a6e3f5"))
					.add(Map.entry(Dispatchor.class, "#c8c8c8"))
					.add(Map.entry(Aggregator.class, "#ff9b86"))
					.build();

	@Builder.Default
	private final List<Map.Entry<Class<?>, MermaidShape>> classToShape = DEFAULT_CLASSTOSHAPE;

	@Builder.Default
	private final List<Map.Entry<Class<?>, String>> classToColor = DEFAULT_CLASSTOCOLOR;

	/**
	 * Names of measures to highlight visually (e.g. to let a human quickly locate specific measures in a large DAG).
	 * Matching nodes receive a thick red border on top of their regular shape/fill styling so the node type remains
	 * readable. Mirrors the {@code highlightedMeasures} option of the Graphviz sibling — but since Mermaid has no
	 * direct equivalent of GraphViz's {@code peripheries=2} (double outline), the emphasis is delivered purely through
	 * a heavier red stroke.
	 */
	@Builder.Default
	private final Set<String> highlightedMeasures = Set.of();

	/**
	 * Renders a whole {@link MeasureForests} as a single Mermaid diagram, using one {@code subgraph} block per forest.
	 *
	 * @param forests
	 *            the set of forests to render
	 * @return the Mermaid {@code flowchart LR} source text
	 */
	public String asMermaid(MeasureForests forests) {
		StringBuilder sb = new StringBuilder(INITIAL_BUFFER_SIZE);
		sb.append("flowchart LR\n");

		// Share the name->id mapping across sub-forests so the same measure name (if present in two
		// forests) keeps the same synthetic id. Edges across forests are not emitted by this class,
		// but reusing ids keeps the diagram coherent if the caller adds any manually.
		NameToId nameToId = new NameToId();
		forests.getNameToForest().forEach((name, forest) -> {
			sb.append("    subgraph \"forest=").append(name).append("\"\n");
			appendForestBody(sb, forest, nameToId, "        ");
			sb.append("    end\n");
		});

		log.debug("mermaid for whole: {}", sb);
		return sb.toString();
	}

	/**
	 * Renders a single {@link IMeasureForest} as a standalone Mermaid diagram.
	 *
	 * @param forest
	 *            the forest to render
	 * @return the Mermaid {@code flowchart LR} source text
	 */
	public String asMermaid(IMeasureForest forest) {
		StringBuilder sb = new StringBuilder(INITIAL_BUFFER_SIZE);
		sb.append("flowchart LR\n");
		appendForestBody(sb, forest, new NameToId(), "    ");

		log.debug("mermaid for {}: {}", forest.getName(), sb);
		return sb.toString();
	}

	/**
	 * Appends node declarations, style lines and edges of a single forest to {@code sb}. Extracted so the single-forest
	 * and multi-forest entry points can share the same rendering logic with different indentation and a shared
	 * {@link NameToId}.
	 *
	 * @param sb
	 *            the target buffer — mutated in-place
	 * @param forest
	 *            the forest whose measures and edges are rendered
	 * @param nameToId
	 *            name-to-id mapping, shared across sub-forests when rendering a {@link MeasureForests}
	 * @param indent
	 *            whitespace prefix placed in front of every emitted line (for nesting inside a {@code subgraph} block)
	 */
	protected void appendForestBody(StringBuilder sb, IMeasureForest forest, NameToId nameToId, String indent) {
		List<IMeasure> measures = ImmutableList.copyOf(forest.getMeasures());

		for (IMeasure measure : measures) {
			String id = nameToId.idFor(measure.getName());

			MermaidShape shape = classToShape.stream()
					.filter(e -> e.getKey().isAssignableFrom(measure.getClass()))
					.map(Map.Entry::getValue)
					.findFirst()
					.orElse(new MermaidShape("(", ")"));

			sb.append(indent)
					.append(id)
					.append(shape.getOpen())
					.append('"')
					.append(measure.getName())
					.append('"')
					.append(shape.getClose())
					.append('\n');

			String fill = classToColor.stream()
					.filter(e -> e.getKey().isAssignableFrom(measure.getClass()))
					.map(Map.Entry::getValue)
					.findFirst()
					.orElse("pink");

			sb.append(indent).append("style ").append(id).append(" fill:").append(fill);
			if (highlightedMeasures.contains(measure.getName())) {
				// Heavier red stroke is the Mermaid equivalent of the GraphViz highlight
				// (color=red, penwidth=3, peripheries=2) — `peripheries` has no direct Mermaid counterpart.
				sb.append(",stroke:red,stroke-width:3px");
			}
			sb.append('\n');
		}

		for (IMeasure measure : measures) {
			if (measure instanceof IHasUnderlyingMeasures hasUnderlyings) {
				String fromId = nameToId.idFor(measure.getName());
				for (String underlying : hasUnderlyings.getUnderlyingNames()) {
					String toId = nameToId.idFor(underlying);
					sb.append(indent).append(fromId).append(" --> ").append(toId).append('\n');
				}
			}
		}
	}

	/**
	 * A pair of Mermaid flowchart delimiters defining a node shape, e.g. {@code ("{{", "}}")} for a hexagon or
	 * {@code ("[(", ")]")} for a cylinder.
	 *
	 * @see <a href="https://mermaid.js.org/syntax/flowchart.html#node-shapes">Mermaid flowchart node shapes</a>
	 */
	@Value
	public static class MermaidShape {
		String open;
		String close;
	}

	/**
	 * Assigns a Mermaid-safe synthetic id ({@code id0}, {@code id1}, …) to each measure name. Mermaid flowchart node
	 * ids must be simple identifiers; measure names routinely contain spaces, dots, parentheses or other characters
	 * that are not valid in an id, so we indirect through a synthetic one and keep the original name in the node label.
	 */
	public static class NameToId {
		private final Map<String, String> idByName = new LinkedHashMap<>();

		/**
		 * Returns the synthetic id associated with {@code name}, creating a new one on the first call.
		 *
		 * @param name
		 *            the measure name
		 * @return the stable Mermaid id for that name
		 */
		public String idFor(String name) {
			return idByName.computeIfAbsent(name, n -> "id" + idByName.size());
		}
	}

}
