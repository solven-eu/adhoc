/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.measure.graphviz;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.measure.forest.IMeasureForest;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.model.Filtrator;
import eu.solven.adhoc.measure.model.Partitionor;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.resource.MeasureForests;
import guru.nidi.graphviz.attribute.Font;
import guru.nidi.graphviz.attribute.Rank;
import guru.nidi.graphviz.engine.Graphviz;
import guru.nidi.graphviz.model.Factory;
import guru.nidi.graphviz.model.MutableGraph;
import guru.nidi.graphviz.model.MutableNode;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

/**
 * Helps to produce `.dot` files, to generate nice graph with GraphViz. This has specific routines to print measure
 * graphs.
 *
 * `.dot` files can be rendered with https://graphviz.org/download/ or at https://dreampuf.github.io/GraphvizOnline/
 *
 * `dot input.dot -Tsvg -o output.svg`
 *
 * @author Benoit Lacelle
 */
@Builder
@Slf4j
public class ForestAsGraphvizDag {
	// IMPORTANT — keep in sync with the Pivotable frontend.
	// The per-class shape/color conventions below are mirrored (as best as Mermaid's flowchart
	// vocabulary allows) in `pivotable/js/src/main/resources/static/ui/js/adhoc-measures-dag.js`,
	// so a measure has the same visual identity in the offline GraphViz DAG and in the in-app
	// Mermaid popup. When you add a new measure class / shape / color here, update that JS file
	// too (look for the `shapeAndStyleForType` function).
	// https://graphviz.org/doc/info/shapes.html
	public static final List<Map.Entry<Class<?>, String>> DEFAULT_CLASSTOSHAPE =
			ImmutableList.<Map.Entry<Class<?>, String>>builder()
					.add(Map.entry(Partitionor.class, "star"))
					.add(Map.entry(Filtrator.class, "invhouse"))
					.add(Map.entry(Dispatchor.class, "msquare"))
					.add(Map.entry(Aggregator.class, "tripleoctagon"))
					.build();
	// https://graphviz.org/doc/info/colors.html
	public static final List<Map.Entry<Class<?>, String>> DEFAULT_CLASSTOCOLOR =
			ImmutableList.<Map.Entry<Class<?>, String>>builder()
					.add(Map.entry(Partitionor.class, "yellow"))
					.add(Map.entry(Filtrator.class, "darkseagreen"))
					.add(Map.entry(Combinator.class, "cyan"))
					.add(Map.entry(Dispatchor.class, "grey"))
					.add(Map.entry(Aggregator.class, "coral"))
					.build();

	@Builder.Default
	private final List<Map.Entry<Class<?>, String>> classToShape = DEFAULT_CLASSTOSHAPE;

	@Builder.Default
	private final List<Map.Entry<Class<?>, String>> classToColor = DEFAULT_CLASSTOCOLOR;

	/**
	 * Names of measures to highlight visually in the generated graph (e.g. to let a human quickly locate specific
	 * measures in a large DAG). Highlighted nodes receive a thick red double-perimeter border, applied on top of the
	 * regular shape/fill-color styling so the node type remains readable.
	 */
	@Builder.Default
	private final Set<String> highlightedMeasures = Set.of();

	private MutableGraph defaultproperties(MutableGraph named) {
		return named.setDirected(true)
				.graphAttrs()
				// We want pre-aggregated measure at the top, and each following measure by going down (as usually done
				// when coding post-processors chains)
				// But LEFT_TO_RIGHT gives much better rendering with dot. (It looks like more a square, than a very
				// thin rectangle)
				.add(Rank.dir(Rank.RankDir.LEFT_TO_RIGHT))
				.nodeAttrs()
				.add(Font.name("arial"))
				.linkAttrs()
				.add("class", "link-class");
	}

	public MutableGraph asGraph(MeasureForests forests) {
		MutableGraph named = Factory.mutGraph("whole");
		MutableGraph g = defaultproperties(named);

		forests.getNameToForest().forEach((name, forest) -> {
			asGraph(forest).addTo(g);
		});

		// log.debug("graphviz for whole: {}", Graphviz.fromGraph(g));

		return g;
	}

	public MutableGraph asGraph(IMeasureForest forest) {
		String forestName = forest.getName();
		MutableGraph named = Factory.mutGraph("forest=" + forestName);
		MutableGraph g = defaultproperties(named);
		g.graphAttrs().add("label", "forest=" + forestName);

		forest.getMeasures().forEach(measure -> {
			MutableNode node = makeNode(measure.getName());

			if (measure instanceof IHasUnderlyingMeasures hasUnderlyingMeasures) {
				List<String> underlyingMeasures = hasUnderlyingMeasures.getUnderlyingNames();
				underlyingMeasures.stream().map(this::makeNode).forEach(node::addLink);
			}

			classToShape.stream()
					.filter(e -> e.getKey().isAssignableFrom(measure.getClass()))
					.map(Map.Entry::getValue)
					.findFirst()
					.ifPresent(shape -> node.add("shape", shape)
							// Fixedsize else stars become gigantic due to large texts
							.add("fixedsize", "true"));

			classToColor.stream()
					.filter(e -> e.getKey().isAssignableFrom(measure.getClass()))
					.map(Map.Entry::getValue)
					.findFirst()
					.ifPresent(color -> node.add("fillcolor", color)
							// https://stackoverflow.com/questions/17252630/why-doesnt-fillcolor-work-with-graphviz
							.add("style", "filled"));

			if (highlightedMeasures.contains(measure.getName())) {
				// `color` is the border color (independent from `fillcolor`), and `peripheries=2` draws a double
				// outline: together they produce a distinctive marker without touching `style` (which is already used
				// for `filled`, and is a single-valued attribute).
				node.add("color", "red").add("penwidth", "3").add("peripheries", "2");
			}

			g.add(node);
		});

		log.debug("graphviz for {}: {}", forestName, Graphviz.fromGraph(g));

		return g;
	}

	protected MutableNode makeNode(String name) {
		// We replace `.` by EOL, to have names looking more like squares, than thin rectangles
		String nicerName = Stream.of(name.split("\\.")).collect(Collectors.joining("\n"));

		return Factory.mutNode(nicerName);
	}

}
