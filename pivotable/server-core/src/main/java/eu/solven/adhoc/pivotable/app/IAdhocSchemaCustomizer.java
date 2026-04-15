package eu.solven.adhoc.pivotable.app;

import eu.solven.adhoc.beta.schema.AdhocSchema;

/**
 * Customizer that can be used to modify the auto-configured {@link AdhocSchema} when its type matches.
 *
 * @param <B>
 *            the builder type
 */
public interface IAdhocSchemaCustomizer<B extends AdhocSchema.AdhocSchemaBuilder> {

	/**
	 * Customize the given builder.
	 * 
	 * @param builder
	 *            the builder to customize
	 * @return the customized builder
	 */
	B customize(B builder);

}
