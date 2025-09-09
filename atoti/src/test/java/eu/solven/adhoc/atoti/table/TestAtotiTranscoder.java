/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package eu.solven.adhoc.atoti.table;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.table.transcoder.CompositeTableAliaser;
import eu.solven.adhoc.table.transcoder.CompositeTableAliaser.ChainMode;
import eu.solven.adhoc.table.transcoder.ITableAliaser;
import eu.solven.adhoc.table.transcoder.MapTableAliaser;

public class TestAtotiTranscoder {
	@Test
	public void testRecursive() {
		// We transcode `AsOf` to `f.AsOf` to explicit which table provides given field in case of ambiguity (e.g. on
		// joins)
		ITableAliaser transcoder = CompositeTableAliaser.builder()
				.chainMode(ChainMode.ApplyAll)
				.transcoder(AtotiAliaser.builder().build())
				.transcoder(MapTableAliaser.builder().queriedToUnderlying("AsOf", "f.AsOf").build())
				.build();

		Assertions.assertThat(transcoder.underlying("someL")).isNull();
		Assertions.assertThat(transcoder.underlying("someL@someH")).isEqualTo("someL");
		Assertions.assertThat(transcoder.underlying("someL@someH@someD")).isEqualTo("someL");

		Assertions.assertThat(transcoder.underlying("AsOf")).isEqualTo("f.AsOf");
		Assertions.assertThat(transcoder.underlying("AsOf@AsOf")).isEqualTo("f.AsOf");
		Assertions.assertThat(transcoder.underlying("AsOf@AsOf@AsOf")).isEqualTo("f.AsOf");

		Assertions.assertThat(transcoder.underlying("unknown")).isNull();
	}

	@Test
	public void testNoPriority() {
		// We transcode `AsOf` to `f.AsOf` to explicit which table provides given field in case of ambiguity (e.g. on
		// joins)
		AtotiAliaser transcoder = AtotiAliaser.builder().build();

		Assertions.assertThat(transcoder.underlying("someL")).isNull();
		Assertions.assertThat(transcoder.underlying("someL@someH")).isEqualTo("someL");
		Assertions.assertThat(transcoder.underlying("someL@someH@someD")).isEqualTo("someL");

		Assertions.assertThat(transcoder.underlying("AsOf")).isNull();
		Assertions.assertThat(transcoder.underlying("AsOf@AsOf")).isEqualTo("AsOf");
		Assertions.assertThat(transcoder.underlying("AsOf@AsOf@AsOf")).isEqualTo("AsOf");

		Assertions.assertThat(transcoder.underlying("unknown")).isNull();
	}
}
