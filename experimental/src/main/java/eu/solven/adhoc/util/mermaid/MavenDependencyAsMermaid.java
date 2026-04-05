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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Scans a multi-module Maven project for {@code pom.xml} files and renders the inter-module dependency graph as a
 * Mermaid {@code flowchart} diagram.
 *
 * <p>
 * Only intra-project dependencies (those sharing the same {@code groupId} as the root module) are shown as edges.
 * External dependencies are ignored. Compile-scope dependencies use a solid arrow ({@code -->}); test-scope
 * dependencies use a dashed arrow ({@code -.->}), making it easy to see which modules are test-only consumers.
 *
 * <p>
 * Modules are grouped into {@code subgraph} blocks by their immediate Maven parent, giving a layered view of the module
 * hierarchy that mirrors the physical directory structure.
 *
 * <p>
 * The output can be pasted at <a href="https://mermaid.live">https://mermaid.live</a>.
 *
 * @author Benoit Lacelle
 */
@Builder
@Slf4j
public class MavenDependencyAsMermaid {

	/** Directory tree to scan for {@code pom.xml} files. */
	Path projectRoot;

	/** Maximum directory depth when walking {@link #projectRoot}. */
	@Default
	@SuppressWarnings("checkstyle:MagicNumber")
	int maxDepth = 10;

	// ── Inner types ──────────────────────────────────────────────────────────

	/** Metadata extracted from a single {@code pom.xml}. */
	@Value
	public static class PomModule {
		/** Effective groupId (falls back to parent groupId when the project element omits it). */
		String groupId;
		String artifactId;
		/** {@code artifactId} of the {@code <parent>} element, or empty if absent. */
		Optional<String> parentArtifactId;
	}

	/** A single dependency declared inside a {@code pom.xml}. */
	@Value
	public static class PomDependency {
		String groupId;
		String artifactId;
		/** Maven scope, e.g. {@code "compile"} (default), {@code "test"}, {@code "provided"}. */
		String scope;

		public boolean isTestScope() {
			return "test".equals(scope);
		}
	}

	/**
	 * An edge in the dependency graph, carrying the Maven scope of the dependency declaration so the Mermaid renderer
	 * can style it accordingly.
	 *
	 * <p>
	 * Deliberately does <em>not</em> override {@code equals}/{@code hashCode} so that JGraphT treats every instance as
	 * a unique edge.
	 */
	public static final class DependencyEdge {
		private final String scope;

		private DependencyEdge(String scope) {
			this.scope = scope;
		}

		public String getScope() {
			return scope;
		}

		public boolean isTestScope() {
			return "test".equals(scope);
		}

		@Override
		public String toString() {
			return scope;
		}
	}

	// ── Public API ───────────────────────────────────────────────────────────

	/**
	 * Parses all {@code pom.xml} files under {@link #projectRoot} and builds a directed dependency graph.
	 *
	 * <p>
	 * Nodes are Maven {@code artifactId}s. Edges run from consumer to dependency (i.e. an edge {@code A → B} means
	 * "module A depends on module B"). Only intra-project dependencies — those whose {@code groupId} matches the root
	 * module's {@code groupId} — create edges.
	 *
	 * @return directed graph of module dependencies
	 * @throws IOException
	 *             if the file system cannot be walked
	 */
	public Graph<String, DependencyEdge> buildGraph() throws IOException {
		List<Path> pomFiles = findPomFiles();
		log.info("Found {} pom.xml files under {}", pomFiles.size(), projectRoot);

		Map<String, PomModule> modules = new LinkedHashMap<>();
		Map<String, List<PomDependency>> dependenciesByModule = new LinkedHashMap<>();

		for (Path pomFile : pomFiles) {
			parsePom(pomFile).ifPresent(pair -> {
				modules.put(pair.module().getArtifactId(), pair.module());
				dependenciesByModule.put(pair.module().getArtifactId(), pair.dependencies());
			});
		}

		String projectGroupId = resolveProjectGroupId(modules);
		log.info("Resolved project groupId: {}", projectGroupId);

		Graph<String, DependencyEdge> graph = new DefaultDirectedGraph<>(null, null, false);
		modules.keySet().forEach(graph::addVertex);

		for (Map.Entry<String, List<PomDependency>> entry : dependenciesByModule.entrySet()) {
			String consumer = entry.getKey();
			for (PomDependency dep : entry.getValue()) {
				if (projectGroupId.equals(dep.getGroupId()) && modules.containsKey(dep.getArtifactId())) {
					graph.addEdge(consumer, dep.getArtifactId(), new DependencyEdge(dep.getScope()));
				}
			}
		}

		log.info("Graph: {} modules, {} dependency edges", graph.vertexSet().size(), graph.edgeSet().size());
		return graph;
	}

	/**
	 * Renders the dependency graph as a Mermaid {@code flowchart TD} string.
	 *
	 * <p>
	 * Modules are grouped into {@code subgraph} blocks by their Maven parent artifactId. Compile-scope edges use a
	 * solid arrow; test-scope edges use a dashed arrow.
	 *
	 * @param graph
	 *            the graph produced by {@link #buildGraph()}
	 * @param modules
	 *            module metadata (needed for the subgraph grouping); pass the map returned by
	 *            {@link #parseAllModules()}
	 * @return Mermaid diagram source text
	 */
	@SuppressWarnings("checkstyle:MagicNumber")
	public String toMermaid(Graph<String, DependencyEdge> graph, Map<String, PomModule> modules) {
		StringBuilder sb = new StringBuilder(512);

		// `LR` as the graph is quite flat
		sb.append("flowchart LR\n");

		// Group modules by parent artifactId for subgraph blocks
		Map<String, List<String>> parentToChildren = new LinkedHashMap<>();
		Set<String> noParent = new LinkedHashSet<>();
		for (String artifactId : graph.vertexSet()) {
			PomModule module = modules.get(artifactId);
			if (module != null && module.getParentArtifactId().isPresent()) {
				String parent = module.getParentArtifactId().get();
				// Only group when the parent itself is NOT in the graph (aggregator-only poms)
				if (!graph.containsVertex(parent)) {
					parentToChildren.computeIfAbsent(parent, k -> new ArrayList<>()).add(artifactId);
				} else {
					noParent.add(artifactId);
				}
			} else {
				noParent.add(artifactId);
			}
		}

		// Standalone nodes (parent is itself a module, or no parent)
		if (!noParent.isEmpty()) {
			for (String artifactId : noParent) {
				sb.append("    ").append(nodeDecl(artifactId)).append('\n');
			}
		}

		// Subgraph blocks for aggregator parents
		for (Map.Entry<String, List<String>> entry : parentToChildren.entrySet()) {
			sb.append("\n    subgraph ").append(entry.getKey()).append('\n');
			for (String artifactId : entry.getValue()) {
				sb.append("        ").append(nodeDecl(artifactId)).append('\n');
			}
			sb.append("    end\n");
		}

		// Dependency edges
		sb.append('\n');
		for (DependencyEdge edge : graph.edgeSet()) {
			String src = nodeId(graph.getEdgeSource(edge));
			String tgt = nodeId(graph.getEdgeTarget(edge));
			if (edge.isTestScope()) {
				sb.append("    ").append(src).append(" -.-> ").append(tgt).append('\n');
			} else {
				sb.append("    ").append(src).append(" --> ").append(tgt).append('\n');
			}
		}

		return sb.toString();
	}

	/**
	 * Convenience overload that parses the modules map internally before rendering.
	 *
	 * @param graph
	 *            the graph produced by {@link #buildGraph()}
	 * @return Mermaid diagram source text
	 * @throws IOException
	 *             if the file system cannot be walked
	 */
	public String toMermaid(Graph<String, DependencyEdge> graph) throws IOException {
		return toMermaid(graph, parseAllModules());
	}

	/**
	 * Returns the {@link PomModule} metadata for every parsed {@code pom.xml}, keyed by {@code artifactId}.
	 *
	 * @throws IOException
	 *             if the file system cannot be walked
	 */
	public Map<String, PomModule> parseAllModules() throws IOException {
		List<Path> pomFiles = findPomFiles();
		Map<String, PomModule> modules = new LinkedHashMap<>();
		for (Path pom : pomFiles) {
			parsePom(pom).ifPresent(p -> modules.put(p.module().getArtifactId(), p.module()));
		}
		return modules;
	}

	// ── Helpers ──────────────────────────────────────────────────────────────

	record ParsedPom(PomModule module, List<PomDependency> dependencies) {
	}

	protected List<Path> findPomFiles() throws IOException {
		try (Stream<Path> walk = Files.walk(projectRoot, maxDepth)) {
			return walk.filter(p -> "pom.xml".equals(p.getFileName().toString()))
					.filter(p -> !p.toString().contains("/target/") && !p.toString().contains("\\target\\"))
					.sorted()
					.toList();
		}
	}

	protected Optional<ParsedPom> parsePom(Path pomFile) {
		Document doc;
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			factory.setNamespaceAware(false);
			doc = factory.newDocumentBuilder().parse(pomFile.toFile());
		} catch (SAXException | IOException | ParserConfigurationException e) {
			log.warn("Failed to parse {}", pomFile, e);
			return Optional.empty();
		}

		Element project = doc.getDocumentElement();

		String artifactId = directChildText(project, "artifactId");
		if (artifactId == null || artifactId.isBlank()) {
			return Optional.empty();
		}

		String groupId = directChildText(project, "groupId");
		Optional<String> parentArtifactId = Optional.empty();

		Element parentElem = directChildElement(project, "parent");
		if (parentElem != null) {
			if (groupId == null || groupId.isBlank()) {
				groupId = directChildText(parentElem, "groupId");
			}
			parentArtifactId = Optional.ofNullable(directChildText(parentElem, "artifactId")).filter(s -> !s.isBlank());
		}

		if (groupId == null) {
			groupId = "";
		}
		PomModule module = new PomModule(groupId, artifactId, parentArtifactId);

		List<PomDependency> deps = new ArrayList<>();
		Element depsElem = directChildElement(project, "dependencies");
		if (depsElem != null) {
			NodeList depNodes = depsElem.getElementsByTagName("dependency");
			for (int i = 0; i < depNodes.getLength(); i++) {
				if (!(depNodes.item(i) instanceof Element depElem)) {
					continue;
				}
				String depGroupId = directChildText(depElem, "groupId");
				String depArtifactId = directChildText(depElem, "artifactId");
				String scope = directChildText(depElem, "scope");
				if (depGroupId != null && depArtifactId != null) {
					if (scope == null) {
						scope = "compile";
					}
					deps.add(new PomDependency(depGroupId, depArtifactId, scope));
				}
			}
		}

		return Optional.of(new ParsedPom(module, deps));
	}

	protected String resolveProjectGroupId(Map<String, PomModule> modules) {
		return modules.values()
				.stream()
				.map(PomModule::getGroupId)
				.filter(g -> g != null && !g.isBlank())
				.findFirst()
				.orElse("");
	}

	/** Mermaid-safe node ID: hyphens replaced by underscores. */
	protected static String nodeId(String artifactId) {
		return artifactId.replace("-", "_").replace(".", "_");
	}

	/** {@code nodeId["artifactId"]} declaration. */
	protected static String nodeDecl(String artifactId) {
		return nodeId(artifactId) + "[\"" + artifactId + "\"]";
	}

	/** Returns the text content of the first direct child element with the given tag name, or {@code null}. */
	protected static String directChildText(Element parent, String tagName) {
		Element child = directChildElement(parent, tagName);
		if (child == null) {
			return null;
		} else {
			return child.getTextContent().trim();
		}
	}

	/** Returns the first direct child {@link Element} with the given tag name, or {@code null}. */
	protected static Element directChildElement(Element parent, String tagName) {
		NodeList children = parent.getChildNodes();
		for (int i = 0; i < children.getLength(); i++) {
			if (children.item(i) instanceof Element e && tagName.equals(e.getTagName())) {
				return e;
			}
		}
		return null;
	}
}
