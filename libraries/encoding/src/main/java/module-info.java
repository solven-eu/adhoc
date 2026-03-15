/**
 * Column-oriented encoding layer: packed integers, dictionary encoding, FSST string compression, and appendable table
 * pages.
 */
module eu.solven.adhoc.encoding {

	// ── Exported packages ────────────────────────────────────────────────────
	exports eu.solven.adhoc.encoding;
	exports eu.solven.adhoc.encoding.column;
	exports eu.solven.adhoc.encoding.column.freezer;
	exports eu.solven.adhoc.encoding.dictionary;
	exports eu.solven.adhoc.encoding.packing;
	exports eu.solven.adhoc.encoding.string;
	exports eu.solven.adhoc.encoding.page;
	exports eu.solven.adhoc.encoding.perfect_hashing;

	// ── Sibling modules ──────────────────────────────────────────────────────
	// adhoc-public: AdhocUnsafe, ILikeList, options.*
	requires eu.solven.adhoc.meta;
	// adhoc-fsst: IFsstDecoder, IFsstConstants, IByteSlice, Utf8ByteSlice
	requires eu.solven.adhoc.fsst;

	// ── Named third-party modules ────────────────────────────────────────────
	requires com.google.common;
	requires org.agrona;
	requires it.unimi.dsi.fastutil;
	// TODO: JavaFastPFOR (me.lemire.integercompression:JavaFastPFOR) has no Automatic-Module-Name,
	// so Maven keeps it on the classpath (unnamed module). Named modules cannot read unnamed modules
	// without a compiler flag. Two options to unblock:
	//   1. Ask upstream to add Automatic-Module-Name=me.lemire.integercompression to their MANIFEST.MF,
	//      then change this line to: requires me.lemire.integercompression;
	//   2. Add to maven-compiler-plugin compilerArgs in pom.xml:
	//        <compilerArg>--add-reads</compilerArg>
	//        <compilerArg>eu.solven.adhoc.encoding=ALL-UNNAMED</compilerArg>
	//      (option 2 does not need a requires directive here)
	// https://github.com/fast-pack/JavaFastPFOR/pull/69
	// requires JavaFastPFOR;

	// Runtime via Lombok @Slf4j
	requires org.slf4j;

	// https://stackoverflow.com/questions/47460373/module-info-java-does-not-work-with-lombok-in-java-9/59976234#59976234
    requires static lombok;

	// Annotation-only (compile-time)
	requires static com.fasterxml.jackson.annotation;
	requires static org.jspecify;
	requires static com.github.spotbugs.annotations;
	requires static com.google.errorprone.annotations;
}
