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
package eu.solven.adhoc.util.mermaid;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.assertj.core.api.Assertions;
import org.jgrapht.Graph;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.util.mermaid.MavenDependencyAsMermaid.DependencyEdge;

/**
 * Unit tests for {@link MavenDependencyAsMermaid}.
 *
 * <p>
 * The test discovers the Maven multi-module project root, builds the full inter-module dependency graph, asserts that
 * key modules and edges are present, and writes the Mermaid diagram to {@code docs/DEPENDENCIES.mmd} at the project
 * root so it can be pasted at <a href="https://mermaid.live">https://mermaid.live</a>.
 *
 * @author Benoit Lacelle
 */
public class TestMavenDependencyAsMermaid {

	/**
	 * Discovers the Maven multi-module project root by walking up from {@code maven.multiModuleProjectDirectory} until
	 * the directory that contains {@code .mvn/} is found.
	 */
	private static Path discoverProjectRoot() {
		Path root = Path.of(System.getProperty("maven.multiModuleProjectDirectory", ".")).toAbsolutePath();
		while (!Files.isDirectory(root.resolve(".mvn"))) {
			Path parent = root.getParent();
			if (parent == null) {
				throw new IllegalStateException("Could not find project root (no .mvn directory found)");
			}
			root = parent;
		}
		return root;
	}

	// ─────────────────────────────────────────────────────────────────────────
	// Full-project graph
	// ─────────────────────────────────────────────────────────────────────────

	/**
	 * Builds the dependency graph for the whole project and writes the Mermaid diagram to
	 * {@code docs/DEPENDENCIES.mmd}.
	 */
	@Test
	public void testGenerateDiagramAndWriteToFile() throws IOException {
		Path projectRoot = discoverProjectRoot();

		MavenDependencyAsMermaid analyzer = MavenDependencyAsMermaid.builder().projectRoot(projectRoot).build();

		Graph<String, DependencyEdge> graph = analyzer.buildGraph();

		// Key modules must be present as vertices
		Assertions.assertThat(graph.vertexSet())
				.contains("adhoc", "adhoc-experimental", "pivotable-server-core", "pivotable-server");

		// adhoc-experimental depends on adhoc (compile scope)
		Assertions.assertThat(graph.containsVertex("adhoc-experimental")).isTrue();
		Assertions.assertThat(graph.containsVertex("adhoc")).isTrue();
		boolean experimentalDependsOnAdhoc = graph.outgoingEdgesOf("adhoc-experimental")
				.stream()
				.anyMatch(e -> "adhoc".equals(graph.getEdgeTarget(e)));
		Assertions.assertThat(experimentalDependsOnAdhoc).as("adhoc-experimental should depend on adhoc").isTrue();

		// pivotable-server depends on pivotable-server-core
		boolean serverDependsOnCore = graph.outgoingEdgesOf("pivotable-server")
				.stream()
				.anyMatch(e -> "pivotable-server-core".equals(graph.getEdgeTarget(e)));
		Assertions.assertThat(serverDependsOnCore)
				.as("pivotable-server should depend on pivotable-server-core")
				.isTrue();

		// Render and write to disk
		String diagram = analyzer.toMermaid(graph);
		Assertions.assertThat(diagram).startsWith("flowchart TD");

		Path outputFile = projectRoot.resolve("docs/DEPENDENCIES.mmd");
		Files.createDirectories(outputFile.getParent());
		Files.writeString(outputFile, diagram);
		Assertions.assertThat(outputFile).isNotEmptyFile();
	}

	// ─────────────────────────────────────────────────────────────────────────
	// Isolated / synthetic parsing tests
	// ─────────────────────────────────────────────────────────────────────────

	/**
	 * A single pom.xml with no dependencies produces a graph with one vertex and no edges.
	 */
	@Test
	public void testSingleModuleNoEdges() throws IOException {
		Path tmpDir = Files.createTempDirectory("maven-mermaid-test");
		String pomContent = """
				<project>
				  <groupId>com.example</groupId>
				  <artifactId>my-module</artifactId>
				  <version>1.0</version>
				</project>
				""";
		Files.writeString(tmpDir.resolve("pom.xml"), pomContent);

		MavenDependencyAsMermaid analyzer = MavenDependencyAsMermaid.builder().projectRoot(tmpDir).build();
		Graph<String, DependencyEdge> graph = analyzer.buildGraph();

		Assertions.assertThat(graph.vertexSet()).containsExactly("my-module");
		Assertions.assertThat(graph.edgeSet()).isEmpty();
	}

	/**
	 * Two modules where one declares an intra-project compile dependency on the other. The resulting graph must contain
	 * one solid edge.
	 */
	@Test
	public void testTwoModulesCompileDependency() throws IOException {
		Path tmpDir = Files.createTempDirectory("maven-mermaid-test");
		Path modA = Files.createDirectories(tmpDir.resolve("module-a"));
		Path modB = Files.createDirectories(tmpDir.resolve("module-b"));

		Files.writeString(modA.resolve("pom.xml"), """
				<project>
				  <groupId>com.example</groupId>
				  <artifactId>module-a</artifactId>
				  <version>1.0</version>
				</project>
				""");

		Files.writeString(modB.resolve("pom.xml"), """
				<project>
				  <groupId>com.example</groupId>
				  <artifactId>module-b</artifactId>
				  <version>1.0</version>
				  <dependencies>
				    <dependency>
				      <groupId>com.example</groupId>
				      <artifactId>module-a</artifactId>
				    </dependency>
				  </dependencies>
				</project>
				""");

		MavenDependencyAsMermaid analyzer = MavenDependencyAsMermaid.builder().projectRoot(tmpDir).build();
		Graph<String, DependencyEdge> graph = analyzer.buildGraph();

		Assertions.assertThat(graph.vertexSet()).containsExactlyInAnyOrder("module-a", "module-b");
		Assertions.assertThat(graph.edgeSet()).hasSize(1);

		DependencyEdge edge = graph.edgeSet().iterator().next();
		Assertions.assertThat(graph.getEdgeSource(edge)).isEqualTo("module-b");
		Assertions.assertThat(graph.getEdgeTarget(edge)).isEqualTo("module-a");
		Assertions.assertThat(edge.isTestScope()).isFalse();
	}

	/**
	 * A test-scope intra-project dependency produces a dashed edge in the Mermaid output.
	 */
	@Test
	public void testTestScopeDependencyProducesDashedEdge() throws IOException {
		Path tmpDir = Files.createTempDirectory("maven-mermaid-test");
		Path modA = Files.createDirectories(tmpDir.resolve("module-a"));
		Path modB = Files.createDirectories(tmpDir.resolve("module-b"));

		Files.writeString(modA.resolve("pom.xml"), """
				<project>
				  <groupId>com.example</groupId>
				  <artifactId>module-a</artifactId>
				  <version>1.0</version>
				</project>
				""");

		Files.writeString(modB.resolve("pom.xml"), """
				<project>
				  <groupId>com.example</groupId>
				  <artifactId>module-b</artifactId>
				  <version>1.0</version>
				  <dependencies>
				    <dependency>
				      <groupId>com.example</groupId>
				      <artifactId>module-a</artifactId>
				      <scope>test</scope>
				    </dependency>
				  </dependencies>
				</project>
				""");

		MavenDependencyAsMermaid analyzer = MavenDependencyAsMermaid.builder().projectRoot(tmpDir).build();
		Graph<String, DependencyEdge> graph = analyzer.buildGraph();

		DependencyEdge edge = graph.edgeSet().iterator().next();
		Assertions.assertThat(edge.isTestScope()).isTrue();

		String diagram = analyzer.toMermaid(graph);
		Assertions.assertThat(diagram).contains("module_b -.-> module_a");
	}

	/**
	 * An external (different groupId) dependency must not produce an edge in the graph.
	 */
	@Test
	public void testExternalDependencyIgnored() throws IOException {
		Path tmpDir = Files.createTempDirectory("maven-mermaid-test");
		Path modA = Files.createDirectories(tmpDir.resolve("module-a"));

		Files.writeString(modA.resolve("pom.xml"), """
				<project>
				  <groupId>com.example</groupId>
				  <artifactId>module-a</artifactId>
				  <version>1.0</version>
				  <dependencies>
				    <dependency>
				      <groupId>org.external</groupId>
				      <artifactId>external-lib</artifactId>
				    </dependency>
				  </dependencies>
				</project>
				""");

		MavenDependencyAsMermaid analyzer = MavenDependencyAsMermaid.builder().projectRoot(tmpDir).build();
		Graph<String, DependencyEdge> graph = analyzer.buildGraph();

		Assertions.assertThat(graph.edgeSet()).isEmpty();
	}

	/**
	 * Modules whose declared Maven parent does not have its own {@code pom.xml} in the scan tree (i.e. it is not a
	 * graph vertex itself) are grouped into a {@code subgraph} block named after that parent.
	 *
	 * <p>
	 * Modules whose parent IS a graph vertex are rendered as standalone nodes instead.
	 */
	@Test
	public void testSubgraphGroupingByAggregatorParent() throws IOException {
		Path tmpDir = Files.createTempDirectory("maven-mermaid-test");
		// No pom.xml for "missing-aggregator" → it will not be a graph vertex
		Path modA = Files.createDirectories(tmpDir.resolve("module-a"));
		Path modB = Files.createDirectories(tmpDir.resolve("module-b"));

		Files.writeString(modA.resolve("pom.xml"), """
				<project>
				  <parent>
				    <groupId>com.example</groupId>
				    <artifactId>missing-aggregator</artifactId>
				    <version>1.0</version>
				  </parent>
				  <artifactId>module-a</artifactId>
				</project>
				""");

		Files.writeString(modB.resolve("pom.xml"), """
				<project>
				  <parent>
				    <groupId>com.example</groupId>
				    <artifactId>missing-aggregator</artifactId>
				    <version>1.0</version>
				  </parent>
				  <artifactId>module-b</artifactId>
				</project>
				""");

		MavenDependencyAsMermaid analyzer = MavenDependencyAsMermaid.builder().projectRoot(tmpDir).build();
		Graph<String, DependencyEdge> graph = analyzer.buildGraph();

		// "missing-aggregator" has no pom.xml → not a vertex → children are grouped into a subgraph
		String diagram = analyzer.toMermaid(graph);
		Assertions.assertThat(diagram).contains("subgraph missing-aggregator");
		Assertions.assertThat(diagram).contains("module_a[\"module-a\"]");
		Assertions.assertThat(diagram).contains("module_b[\"module-b\"]");
	}

	/**
	 * {@link MavenDependencyAsMermaid#nodeId} replaces hyphens with underscores and dots with underscores.
	 */
	@Test
	public void testNodeIdSanitization() {
		Assertions.assertThat(MavenDependencyAsMermaid.nodeId("my-module")).isEqualTo("my_module");
		Assertions.assertThat(MavenDependencyAsMermaid.nodeId("a.b.c")).isEqualTo("a_b_c");
		Assertions.assertThat(MavenDependencyAsMermaid.nodeId("no-change")).isEqualTo("no_change");
	}

	/**
	 * {@link MavenDependencyAsMermaid#nodeDecl} wraps the original artifactId in quotes for Mermaid labels.
	 */
	@Test
	public void testNodeDecl() {
		Assertions.assertThat(MavenDependencyAsMermaid.nodeDecl("my-module")).isEqualTo("my_module[\"my-module\"]");
	}
}
