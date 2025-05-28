package eu.solven.adhoc.measure.dynamic_tenors;

public interface IExamplePnLExplainConstant {
	// Time left for given contract
	String K_TENOR = "tenor";
	// Initial duration for given contract (e.g. tenor at inception)
	String K_MATURITY = "maturity";

	// Some SUM sensitivity/risk/measure
	String DELTA = "delta";
}
