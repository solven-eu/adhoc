/**
 * Routing measures: dispatch a {@link eu.solven.adhoc.engine.step.CubeQueryStep} to one or more underlying steps via a
 * caller-provided {@link eu.solven.adhoc.measure.routing.IRoutingLogic}, then coalesce their per-slice values. Useful
 * in migration scenarios (typically Atoti → Adhoc) where a logical measure must be backed by different physical
 * measures or filters depending on the slice — e.g. legacy table for data before a modelling cutoff, modern table
 * afterwards.
 *
 * <p>
 * All types in this package are null-marked: parameters, return types and fields are non-null by default; explicit
 * {@link org.jspecify.annotations.Nullable @Nullable} marks the opt-outs.
 */
@NullMarked
package eu.solven.adhoc.measure.routing;

import org.jspecify.annotations.NullMarked;
