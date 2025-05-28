package eu.solven.adhoc.measure.dynamic_tenors;

import java.util.Optional;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.row.ISlicedRecord;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import lombok.extern.slf4j.Slf4j;

/**
 * Simulate a service providing a MarketData shift (i.e. the variation (in nominal) of a MarketData between 2 given
 * instants).
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class MarketDataShiftCombination implements ICombination, IExamplePnLExplainConstant {
	@Override
	public IValueProvider combine(ISliceWithStep slice, ISlicedRecord slicedRecord) {
		Optional<String> optTenor =
				EqualsMatcher.extractOperand(FilterHelpers.getValueMatcher(slice.asFilter(), K_TENOR), String.class);
		if (optTenor.isEmpty()) {
			return IValueProvider.setValue("Lack tenor");
		}
		Optional<String> optMaturity =
				EqualsMatcher.extractOperand(FilterHelpers.getValueMatcher(slice.asFilter(), K_MATURITY), String.class);
		if (optMaturity.isEmpty()) {
			return IValueProvider.setValue("Lack maturity");
		}

		// [-99, 99]
		int deterministicRandomHash = (optTenor.get() + optMaturity.get()).hashCode() % 100;
		// [-9.9, 9.9]
		double deterministicRandomMarketDataShift = deterministicRandomHash / 10D;

		return IValueProvider.setValue(deterministicRandomMarketDataShift);
	}
}
