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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.jgrapht.Graph;
import org.jgrapht.alg.connectivity.ConnectivityInspector;
import org.jgrapht.graph.DirectedMultigraph;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.FieldDeclaration;
import com.github.javaparser.ast.body.TypeDeclaration;
import com.github.javaparser.ast.expr.Expression;
import com.github.javaparser.ast.expr.FieldAccessExpr;
import com.github.javaparser.ast.expr.MethodCallExpr;
import com.github.javaparser.ast.expr.NameExpr;
import com.github.javaparser.ast.expr.ObjectCreationExpr;
import com.github.javaparser.ast.type.ClassOrInterfaceType;
import com.google.common.collect.ImmutableList;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Singular;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Builds a JGraphT class-hierarchy graph from Java source files (via JavaParser) and renders it to Mermaid.
 *
 * <p>
 * Unlike the reflection-based ClassHierarchyAsMermaid, this implementation reads source code directly, capturing field
 * initializers to show the <em>actual default wiring</em>.
 *
 * <ul>
 * <li>For fields <strong>with a default value</strong> (either a {@code @Builder.Default} annotation or any field
 * initializer), the composition edge targets the <strong>concrete default class</strong>; the declared interface type
 * is shown as a hint inside the owning class box.</li>
 * <li>For fields <strong>without a default value</strong>, the composition edge targets the declared interface type,
 * and <strong>all known implementations</strong> found in the source are added as nodes below the interface.</li>
 * <li>Interface nodes are only added when they appear as a field type — superinterfaces are <strong>not</strong> added
 * automatically.</li>
 * </ul>
 *
 * <p>
 * The intermediate JGraphT {@link Graph} can be inspected or filtered before calling {@link #toMermaid(Graph)}. See
 * https://jgrapht.org/javadoc/org.jgrapht.core/org/jgrapht/Graph.html
 *
 * @author Benoit Lacelle
 */
@Builder
@Slf4j
public class ClassHierarchyAsJavaParserMermaid {

	// ── Configuration ────────────────────────────────────────────────────────

	/** Source roots to scan for Java source files (e.g. {@code src/main/java} directories). */
	@Singular
	List<Path> sourceRoots;

	/** Default value for {@link #maxDepth}. */
	static final int DEFAULT_MAX_DEPTH = 40;

	/** Maximum directory depth when searching for {@code src/main/java} roots under the project directory. */
	static final int SOURCE_ROOT_SEARCH_DEPTH = 30;

	/** Initial capacity for the {@link StringBuilder} used when building the Mermaid diagram output. */
	static final int DIAGRAM_BUILDER_INITIAL_CAPACITY = 512;

	/** Maximum recursion depth from the root class. */
	@Default
	int maxDepth = DEFAULT_MAX_DEPTH;

	// ── Inner types ─────────────────────────────────────────────────────────

	/** A vertex in the class-hierarchy graph, representing a Java type. */
	@Value
	public static class ClassNode {
		String simpleName;
		boolean isInterface;
		boolean isAbstract;
	}

	/** Kind of directed edge in the class-hierarchy graph. */
	public enum EdgeKind {
		/** Class/interface implements or extends an interface. */
		IMPLEMENTS,
		/** Class extends a class. */
		EXTENDS,
		/** A class has a field whose (declared or default) type is the target. */
		HAS_FIELD
	}

	/**
	 * An edge in the class-hierarchy graph.
	 *
	 * <p>
	 * Deliberately does <strong>not</strong> override {@code equals}/{@code hashCode} so that JGraphT's
	 * {@link DirectedMultigraph} treats every instance as a unique edge (required to support multiple edges between the
	 * same pair of nodes).
	 */
	public static final class ClassEdge {
		private final EdgeKind kind;
		/** Non-null for {@link EdgeKind#HAS_FIELD}. */
		private final String fieldName;
		/**
		 * For {@link EdgeKind#HAS_FIELD}: the interface type declared in the source (even when the composition target
		 * is the concrete default class).
		 */
		private final String declaredType;

		private ClassEdge(EdgeKind kind, String fieldName, String declaredType) {
			this.kind = kind;
			this.fieldName = fieldName;
			this.declaredType = declaredType;
		}

		public EdgeKind getKind() {
			return kind;
		}

		public String getFieldName() {
			return fieldName;
		}

		public String getDeclaredType() {
			return declaredType;
		}

		static ClassEdge implementing() {
			return new ClassEdge(EdgeKind.IMPLEMENTS, null, null);
		}

		static ClassEdge extending() {
			return new ClassEdge(EdgeKind.EXTENDS, null, null);
		}

		static ClassEdge field(String fieldName, String declaredType) {
			return new ClassEdge(EdgeKind.HAS_FIELD, fieldName, declaredType);
		}

		@Override
		public String toString() {
			if (kind == EdgeKind.HAS_FIELD) {
				return "HAS_FIELD(" + fieldName + "_" + declaredType + ")";
			} else {
				return kind.name();
			}
		}
	}

	// ── Public API ───────────────────────────────────────────────────────────

	/**
	 * Builds a JGraphT directed multigraph of the class hierarchy starting from {@code rootClassName}.
	 *
	 * <p>
	 * Rules for field traversal:
	 * <ul>
	 * <li><b>Field with a default value</b> (either a {@code @Builder.Default} annotation or any field initializer) —
	 * both the declared interface type and the concrete default class are added as nodes; the composition edge targets
	 * the <strong>concrete class</strong>, and the interface type is shown as a hint in the owning class box.</li>
	 * <li><b>Field without a default value</b> — the declared interface type is added, together with all concrete
	 * classes that directly implement it (found in the parsed sources); the composition edge targets the
	 * interface.</li>
	 * </ul>
	 *
	 * @param rootClassName
	 *            simple class name to start from (e.g. {@code "CubeWrapper"})
	 * @return a manipulable JGraphT graph
	 */
	public Graph<ClassNode, ClassEdge> buildGraph(String rootClassName) {
		// load classes ASTs
		Map<String, TypeDeclaration<?>> nameToDecl = parseAllSources();
		// map from interfaces to classes
		Map<String, List<String>> interfaceToImpls = buildInterfaceToImpls(nameToDecl);

		// Phase 1: BFS to decide which class/interface names to include
		Map<String, ClassNode> nameToNode = new LinkedHashMap<>();
		collectNodes(rootClassName, 0, nameToNode, nameToDecl, interfaceToImpls);

		// Phase 2: build the JGraphT graph vertices
		Graph<ClassNode, ClassEdge> graph = new DirectedMultigraph<>(null, null, false);
		nameToNode.values().forEach(graph::addVertex);

		// Phase 3: add edges for every collected node
		for (String name : nameToNode.keySet()) {
			TypeDeclaration<?> decl = nameToDecl.get(name);
			if (!(decl instanceof ClassOrInterfaceDeclaration coid)) {
				continue;
			}
			ClassNode sourceNode = nameToNode.get(name);

			// IMPLEMENTS / EXTENDS — only when the target is also in the graph
			coid.getImplementedTypes().forEach(t -> {
				ClassNode targetNode = nameToNode.get(rawName(t.getNameAsString()));
				if (targetNode != null) {
					graph.addEdge(sourceNode, targetNode, ClassEdge.implementing());
				}
			});
			coid.getExtendedTypes().forEach(t -> {
				ClassNode targetNode = nameToNode.get(rawName(t.getNameAsString()));
				if (targetNode != null) {
					graph.addEdge(sourceNode, targetNode, ClassEdge.extending());
				}
			});

			// HAS_FIELD — concrete classes only (interfaces have no instance fields)
			if (!coid.isInterface()) {
				coid.getFields().forEach(field -> {
					if (field.isStatic()) {
						return;
					}
					String declaredType = rawName(field.getVariable(0).getType().asString());
					String fieldName = field.getVariable(0).getNameAsString();

					// Composition target: concrete default class (when a default value exists) or declared interface
					// type
					String compositionTarget = declaredType;
					if (hasDefaultValue(field)) {
						String defaultClass =
								field.getVariable(0).getInitializer().map(this::extractFirstClassName).orElse(null);
						if (defaultClass != null && nameToNode.containsKey(declaredType)
								&& nameToNode.containsKey(defaultClass)) {
							compositionTarget = defaultClass;
						}
					}

					ClassNode targetNode = nameToNode.get(compositionTarget);
					if (targetNode != null) {
						graph.addEdge(sourceNode, targetNode, ClassEdge.field(fieldName, declaredType));
					}
				});
			}
		}

		removeUnreferencedInterfaces(graph);
		removeDisconnectedFromRoot(graph, nameToNode.get(rootClassName));

		int componentCount = new ConnectivityInspector<>(graph).connectedSets().size();
		log.info("Graph has {} vertices, {} edges, {} connected component(s)",
				graph.vertexSet().size(),
				graph.edgeSet().size(),
				componentCount);

		return graph;
	}

	/**
	 * Removes all vertices that are not in the same weakly-connected component as {@code root}.
	 *
	 * <p>
	 * Any cluster that has no path to or from the root (ignoring edge direction) is dropped entirely. This keeps the
	 * diagram focused on types that are actually reachable from the entry point.
	 *
	 * @param graph
	 *            the graph produced by {@link #buildGraph(String)}, modified in place
	 * @param root
	 *            the root node; if {@code null} (root class not found) the graph is left unchanged
	 */
	public static void removeDisconnectedFromRoot(Graph<ClassNode, ClassEdge> graph, ClassNode root) {
		if (root == null) {
			return;
		}
		Set<ClassNode> rootComponent = new ConnectivityInspector<>(graph).connectedSetOf(root);
		graph.vertexSet().stream().filter(n -> !rootComponent.contains(n)).toList().forEach(graph::removeVertex);
	}

	/**
	 * Removes interface nodes that are not the target of any {@link EdgeKind#HAS_FIELD} edge.
	 *
	 * <p>
	 * An interface is only meaningful in the diagram when at least one class declares a field of that interface type.
	 * Interfaces that appear solely via {@code implements} relationships (but are never used as a field type) are
	 * removed together with all their edges.
	 *
	 * @param graph
	 *            the graph produced by {@link #buildGraph(String)}, modified in place
	 */
	public static void removeUnreferencedInterfaces(Graph<ClassNode, ClassEdge> graph) {
		graph.vertexSet()
				.stream()
				.filter(ClassNode::isInterface)
				.filter(n -> graph.incomingEdgesOf(n).stream().noneMatch(e -> e.getKind() == EdgeKind.HAS_FIELD))
				.toList()
				.forEach(graph::removeVertex);
	}

	/**
	 * Renders a JGraphT class-hierarchy graph to a Mermaid {@code classDiagram} string.
	 *
	 * <p>
	 * The output can be pasted at https://mermaid.live
	 *
	 * @param graph
	 *            the graph produced by {@link #buildGraph(String)}
	 * @return Mermaid diagram source text
	 */
	public String toMermaid(Graph<ClassNode, ClassEdge> graph) {
		StringBuilder sb = new StringBuilder(DIAGRAM_BUILDER_INITIAL_CAPACITY);
		sb.append(
				"classDiagram\n    direction TB\n\n    %% ─── Interfaces ────────────────────────────────────────────────\n");
		graph.vertexSet()
				.stream()
				.sorted(Comparator.comparing(ClassNode::getSimpleName))
				.filter(ClassNode::isInterface)
				.forEach(node -> appendNodeDeclaration(sb, node, graph));

		sb.append("\n    %% ─── Concrete classes ──────────────────────────────────────────\n");
		graph.vertexSet()
				.stream()
				.sorted(Comparator.comparing(ClassNode::getSimpleName))
				.filter(n -> !n.isInterface() && !n.isAbstract())
				.forEach(node -> appendNodeDeclaration(sb, node, graph));

		boolean hasAbstract = graph.vertexSet().stream().anyMatch(n -> !n.isInterface() && n.isAbstract());
		if (hasAbstract) {
			sb.append("\n    %% ─── Abstract classes ──────────────────────────────────────────\n");
			graph.vertexSet()
					.stream()
					.sorted(Comparator.comparing(ClassNode::getSimpleName))
					.filter(n -> !n.isInterface() && n.isAbstract())
					.forEach(node -> appendNodeDeclaration(sb, node, graph));
		}

		Set<String> implementsRels = new LinkedHashSet<>();
		Set<String> compositionRels = new LinkedHashSet<>();
		graph.edgeSet().stream().sorted(Comparator.comparing(ClassEdge::toString)).forEach(edge -> {
			ClassNode source = graph.getEdgeSource(edge);
			ClassNode target = graph.getEdgeTarget(edge);
			EdgeKind kind = edge.getKind();
			if (kind == EdgeKind.IMPLEMENTS) {
				implementsRels.add(target.getSimpleName() + " <|.. " + source.getSimpleName());
			} else if (kind == EdgeKind.EXTENDS) {
				implementsRels.add(target.getSimpleName() + " <|-- " + source.getSimpleName());
			} else if (kind == EdgeKind.HAS_FIELD) {
				compositionRels
						.add(source.getSimpleName() + " *-- " + target.getSimpleName() + " : " + edge.getFieldName());
			}
		});

		if (!implementsRels.isEmpty()) {
			sb.append("\n    %% ─── Implements / Extends ──────────────────────────────────────\n");
			implementsRels.forEach(r -> sb.append("    ").append(r).append('\n'));
		}
		if (!compositionRels.isEmpty()) {
			sb.append("\n    %% ─── Composition ───────────────────────────────────────────────\n");
			compositionRels.forEach(r -> sb.append("    ").append(r).append('\n'));
		}

		return sb.toString();
	}

	// ── Graph building helpers ───────────────────────────────────────────────

	private void appendNodeDeclaration(StringBuilder sb, ClassNode node, Graph<ClassNode, ClassEdge> graph) {
		sb.append("    class ").append(node.getSimpleName()).append(" {\n");
		// List HAS_FIELD edges as field entries using the declared type for display
		graph.outgoingEdgesOf(node)
				.stream()
				.filter(e -> e.getKind() == EdgeKind.HAS_FIELD)
				.sorted(Comparator.comparing(ClassEdge::getFieldName))
				.forEach(e -> sb.append("        ")
						.append(e.getDeclaredType())
						.append(' ')
						.append(e.getFieldName())
						.append('\n'));
		sb.append("    }\n");
	}

	/**
	 * BFS: adds class nodes reachable from {@code className} within {@link #maxDepth} hops.
	 *
	 * <p>
	 * Only fields drive recursion — {@code implements}/{@code extends} chains are <em>not</em> followed for node
	 * collection, so parent interfaces only appear when explicitly used as a field type.
	 */
	private void collectNodes(String className,
			int depth,
			Map<String, ClassNode> visited,
			Map<String, TypeDeclaration<?>> nameToDecl,
			Map<String, List<String>> interfaceToImpls) {
		if (depth > maxDepth || visited.containsKey(className)) {
			return;
		}
		TypeDeclaration<?> decl = nameToDecl.get(className);
		if (decl == null) {
			return;
		}

		boolean isInterface = false;
		boolean isAbstract = false;
		if (decl instanceof ClassOrInterfaceDeclaration coid) {
			isInterface = coid.isInterface();
			isAbstract = !isInterface && coid.isAbstract();
		}
		visited.put(className, new ClassNode(className, isInterface, isAbstract));

		if (!(decl instanceof ClassOrInterfaceDeclaration coid) || coid.isInterface()) {
			return;
		}

		coid.getFields().forEach(field -> {
			if (field.isStatic()) {
				return;
			}
			String declaredType = rawName(field.getVariable(0).getType().asString());

			if (hasDefaultValue(field)) {
				// Has a default: add the declared type (interface) and, when it is a project type, the concrete default
				// class
				collectNodes(declaredType, depth + 1, visited, nameToDecl, interfaceToImpls);
				field.getVariable(0).getInitializer().ifPresent(init -> {
					String defaultClass = extractFirstClassName(init);
					if (defaultClass != null && nameToDecl.containsKey(declaredType)) {
						collectNodes(defaultClass, depth + 1, visited, nameToDecl, interfaceToImpls);
					}
				});
			} else {
				// No default: add the declared type and all known implementations found in sources
				collectNodes(declaredType, depth + 1, visited, nameToDecl, interfaceToImpls);
				interfaceToImpls.getOrDefault(declaredType, ImmutableList.of())
						.forEach(impl -> collectNodes(impl, depth + 1, visited, nameToDecl, interfaceToImpls));
			}

			// Also follow the first generic type argument (e.g. ImmutableList<IMeasure> → IMeasure)
			if (field.getVariable(0).getType() instanceof ClassOrInterfaceType cit) {
				cit.getTypeArguments().ifPresent(args -> args.forEach(arg -> {
					if (arg instanceof ClassOrInterfaceType argCit) {
						collectNodes(rawName(
								argCit.getNameAsString()), depth + 1, visited, nameToDecl, interfaceToImpls);
					}
				}));
			}
		});
	}

	/**
	 * Parses all {@code .java} files under each configured {@link #sourceRoots}.
	 *
	 * @return map from simple class name to its parsed {@link TypeDeclaration}
	 */
	Map<String, TypeDeclaration<?>> parseAllSources() {
		Map<String, TypeDeclaration<?>> result = new LinkedHashMap<>();
		JavaParser parser = new JavaParser();
		for (Path root : sourceRoots) {
			if (!Files.exists(root)) {
				log.warn("Source root does not exist: {}", root);
				continue;
			}
			try (Stream<Path> files = Files.walk(root)) {
				files.filter(p -> p.toString().endsWith(".java")).sorted().forEach(javaFile -> {
					try {
						parser.parse(javaFile).getResult().stream().flatMap(cu -> cu.getTypes().stream()).flatMap(t -> {
							if (t.isClassOrInterfaceDeclaration()) {
								return Stream.concat(Stream.of(t),
										t.getMembers()
												.stream()
												.filter(b -> b.isClassOrInterfaceDeclaration())
												.map(b -> b.asClassOrInterfaceDeclaration()));
							} else {
								return Stream.of(t);
							}
						}).forEach(t -> result.put(t.getNameAsString(), t));
					} catch (IOException e) {
						log.warn("Failed to parse {}", javaFile, e);
					}
				});
			} catch (IOException e) {
				log.warn("Failed to walk source root {}", root, e);
			}
		}
		log.info("Parsed {} types from {} source roots", result.size(), sourceRoots.size());
		return result;
	}

	/**
	 * Scans parsed sources to build a map from interface simple name to the list of concrete classes that directly
	 * declare {@code implements InterfaceName}.
	 */
	private static Map<String, List<String>> buildInterfaceToImpls(Map<String, TypeDeclaration<?>> nameToDecl) {
		Map<String, List<String>> result = new LinkedHashMap<>();
		nameToDecl.values().forEach(decl -> {
			if (!(decl instanceof ClassOrInterfaceDeclaration coid) || coid.isInterface()) {
				return;
			}
			coid.getImplementedTypes()
					.forEach(t -> result.computeIfAbsent(rawName(t.getNameAsString()), k -> new ArrayList<>())
							.add(coid.getNameAsString()));
		});
		// Sort each implementation list so that traversal order in collectNodes is deterministic
		result.values().forEach(list -> list.sort(Comparator.naturalOrder()));
		return result;
	}

	private boolean hasDefaultAnnotation(FieldDeclaration field) {
		return field.getAnnotations().stream().anyMatch(a -> {
			String name = a.getNameAsString();
			return "Default".equals(name) || "Builder.Default".equals(name);
		});
	}

	/**
	 * Returns {@code true} when the field carries a known default value, either via a {@code @Builder.Default}
	 * annotation or via a plain field initializer (e.g. {@code private IFoo foo = new ConcreteA()}).
	 */
	private boolean hasDefaultValue(FieldDeclaration field) {
		return hasDefaultAnnotation(field) || field.getVariable(0).getInitializer().isPresent();
	}

	/**
	 * Extracts the leftmost class name from an initializer expression.
	 *
	 * <ul>
	 * <li>{@code CubeQueryEngine.builder().build()} → {@code "CubeQueryEngine"}</li>
	 * <li>{@code new ColumnsManager()} → {@code "ColumnsManager"}</li>
	 * </ul>
	 */
	private String extractFirstClassName(Expression expr) {
		if (expr instanceof MethodCallExpr mce) {
			return mce.getScope().map(this::extractFirstClassName).orElse(null);
		} else if (expr instanceof ObjectCreationExpr oce) {
			return oce.getType().getNameAsString();
		} else if (expr instanceof NameExpr ne) {
			return ne.getNameAsString();
		} else if (expr instanceof FieldAccessExpr fae) {
			return extractFirstClassName(fae.getScope());
		}
		return null;
	}

	/** Strips generic type parameters: {@code "List<String>"} → {@code "List"}. */
	private static String rawName(String typeName) {
		int idx = typeName.indexOf('<');
		if (idx >= 0) {
			return typeName.substring(0, idx);
		} else {
			return typeName;
		}
	}

}
