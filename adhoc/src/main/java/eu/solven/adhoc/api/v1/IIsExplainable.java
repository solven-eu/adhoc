package eu.solven.adhoc.api.v1;

/**
 * Some components can have their queryPlan explained. If true, we will spent some time providing information about the
 * queryPlan.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IIsExplainable {

	boolean isExplain();
}
