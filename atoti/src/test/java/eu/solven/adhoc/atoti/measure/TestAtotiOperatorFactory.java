package eu.solven.adhoc.atoti.measure;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.measure.operator.StandardOperatorFactory;

public class TestAtotiOperatorFactory {
	@Test
	public void testWithRoot() {
		IOperatorFactory rootOperatorFactory = Mockito.mock(IOperatorFactory.class);
		StandardOperatorFactory atotiFactory = AtotiOperatorFactory.builder().build();
		Assertions.assertThat(atotiFactory).isInstanceOf(AtotiOperatorFactory.class);

		IOperatorFactory withRoot = atotiFactory.withRoot(rootOperatorFactory);

		Assertions.assertThat(withRoot).isInstanceOf(AtotiOperatorFactory.class);
	}
}
