/**
 * {@link eu.solven.adhoc.engine.observability.plan.QueryPlan} mutation events. The engine emits these via the shared
 * {@code IAdhocEventBus}; the {@code QueryPlanRegistryUpdater} subscribes and mutates the registry's plan in place. The
 * event types are also the wire format for a future UI streaming bridge.
 *
 * <p>
 * All types in this package are null-marked.
 */
@NullMarked
package eu.solven.adhoc.engine.observability.plan.events;

import org.jspecify.annotations.NullMarked;
