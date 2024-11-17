package eu.solven.adhoc.api.v1;

/**
 * A aggregation query. It is configured by:
 * 
 * - a filtering condition - axes along which the result is sliced - aggregations accumulating some measures
 * 
 * @author Benoit Lacelle
 *
 */
public interface IAdhocQuery extends IWhereGroupbyAdhocQuery, IHasRefMeasures, IIsExplainable {

}