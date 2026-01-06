package eu.solven.adhoc.dictionary.page;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAppendableTablePage {
	@Test
	public void testCapacity() {
		AppendableTablePage page = AppendableTablePage.builder().capacity(2).build();

		Assertions.assertThat(page.pollNextRowIndex()).isEqualTo(0);
		Assertions.assertThat(page.pollNextRowIndex()).isEqualTo(1);
		Assertions.assertThat(page.pollNextRowIndex()).isEqualTo(-1);
		Assertions.assertThat(page.pollNextRowIndex()).isEqualTo(-1);
	}
}
