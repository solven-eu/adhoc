package eu.solven.adhoc.measure.model;

import java.util.Objects;

import com.google.common.collect.Lists;

import eu.solven.adhoc.measure.ReferencedMeasure;
import eu.solven.adhoc.measure.combination.CoalesceCombination;
import lombok.experimental.UtilityClass;

@UtilityClass
public class MeasureHelpers {

	/**
	 * 
	 * @param name
	 * @param underlying
	 * @param moreUnderlyings
	 * @return an IMeasure doing the SUM of underlyings.
	 */
	public static IMeasure sum(String name, String underlying, String... moreUnderlyings) {
		if (moreUnderlyings.length == 0) {
			return alias(name, underlying);
		} else {
			return Combinator.builder().name(name).underlyings(Lists.asList(underlying, moreUnderlyings)).build();
		}
	}

	/**
	 * 
	 * @param alias
	 *            the name of the name, i.e. the name with which one can refer to this alias.
	 * @param aliased
	 *            the name of the aliased measure, i.e. the name of the underlying/actually defined measure
	 * @return
	 */
	public static IMeasure alias(String alias, String aliased) {
		if (Objects.equals(alias, aliased)) {
			return ReferencedMeasure.ref(alias);
		} else {
			return Combinator.builder().name(alias).combinationKey(CoalesceCombination.KEY).underlying(aliased).build();
		}
	}
}
