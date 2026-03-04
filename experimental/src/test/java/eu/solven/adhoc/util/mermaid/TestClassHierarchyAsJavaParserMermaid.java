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
package eu.solven.adhoc.util.mermaid;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.jgrapht.Graph;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.util.mermaid.ClassHierarchyAsJavaParserMermaid.ClassEdge;
import eu.solven.adhoc.util.mermaid.ClassHierarchyAsJavaParserMermaid.ClassNode;

public class TestClassHierarchyAsJavaParserMermaid {

	/**
	 * Discovers all {@code src/main/java} directories under the multi-module project root.
	 *
	 * <p>
	 * Maven sets {@code maven.multiModuleProjectDirectory} to the top-level project directory. See
	 * https://maven.apache.org/configure.html#maven-multimoduleprojectdirectory
	 */
	private static List<Path> discoverSourceRoots() throws IOException {
		Path projectRootInitial = Path.of(System.getProperty("maven.multiModuleProjectDirectory", "."));
		Path projectRoot = projectRootInitial.toAbsolutePath();

		while (!Files.isDirectory(projectRoot.resolve(".mvn"))) {
			Path parentPath = projectRoot.getParent();
			if (parentPath == null) {
				throw new IllegalStateException("Issue looking for root given " + projectRootInitial);
			}
			projectRoot = parentPath;
		}

		try (Stream<Path> walk = Files.walk(projectRoot, 128)) {
			return walk.filter(p -> p.endsWith("src/main/java"))
					.filter(p -> !p.toString().contains("/target"))
					.filter(Files::isDirectory)
					.toList();
		}
	}

	/**
	 * Prints a Mermaid {@code classDiagram} rooted at {@code CubeWrapper} to stdout.
	 *
	 * <p>
	 * Source roots are discovered automatically under the Maven multi-module project root. The output can be pasted at
	 * https://mermaid.live
	 */
	@Test
	public void testGenerateDiagramAndWriteToFile() throws IOException {
		ClassHierarchyAsJavaParserMermaid analyzer =
				ClassHierarchyAsJavaParserMermaid.builder().sourceRoots(discoverSourceRoots()).build();

		Graph<ClassNode, ClassEdge> graph = analyzer.buildGraph("CubeWrapper");

		// CubeWrapper itself must be a node
		// Assertions.assertThat(graph.vertexSet()).extracting(ClassNode::getSimpleName).contains("CubeWrapper");

		// // Fields with @Default must produce a HAS_FIELD edge to the concrete default class, not the interface
		// Assertions.assertThat(graph.edgeSet())
		// .filteredOn(e -> e.getKind() == EdgeKind.HAS_FIELD)
		// .filteredOn(e -> "engine".equals(e.getFieldName()))
		// .extracting(e -> graph.getEdgeTarget(e).getSimpleName())
		// .containsExactly("CubeQueryEngine");
		//
		// // Fields without @Default must expose available implementations
		// Assertions.assertThat(graph.vertexSet()).extracting(ClassNode::getSimpleName).contains("ITableWrapper");
		//
		String diagram = analyzer.toMermaid(graph);
		//
		// Assertions.assertThat(diagram)
		// .contains("CubeWrapper")
		// .contains("CubeQueryEngine")
		// .contains("ITableWrapper")
		// .contains("engine");

		// Write the generated diagram to the project root so it can be rendered at https://mermaid.live
		Path projectRoot = Path.of(System.getProperty("maven.multiModuleProjectDirectory", ".."));
		Path outputFile = projectRoot.resolve("ARCHITECTURE.mmd");
		Files.writeString(outputFile, diagram);

		Assertions.assertThat(outputFile).isNotEmptyFile();
	}
}
