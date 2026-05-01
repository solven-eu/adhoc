/**
 * Column freezer strategies: turn appendable columns into immutable, optimised snapshots.
 *
 * <p>
 * All types in this package are null-marked: parameters, return types and fields are non-null by default; explicit
 * {@link org.jspecify.annotations.Nullable @Nullable} marks the opt-outs.
 */
@NullMarked
package eu.solven.adhoc.encoding.column.freezer;

import org.jspecify.annotations.NullMarked;
