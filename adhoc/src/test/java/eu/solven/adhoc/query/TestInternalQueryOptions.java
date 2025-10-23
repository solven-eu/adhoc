package eu.solven.adhoc.query;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

import eu.solven.pepper.unittest.PepperJacksonTestHelper;

public class TestInternalQueryOptions {

	@Disabled("TODO")
	@Test
	public void testJackson() throws JsonProcessingException {
		String option = PepperJacksonTestHelper.verifyJackson(IQueryOption.class,
				InternalQueryOptions.ONE_TABLE_QUERY_PER_INDUCER);

		Assertions.assertThat(option).isEqualTo("""
				"EXPLAIN"
				""".trim());
	}

}
