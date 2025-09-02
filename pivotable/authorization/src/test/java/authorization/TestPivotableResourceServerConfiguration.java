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
package authorization;

import java.text.ParseException;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import com.nimbusds.jose.jwk.OctetSequenceKey;

import eu.solven.adhoc.pivotable.oauth2.IPivotableOAuth2Constants;
import eu.solven.adhoc.pivotable.oauth2.resourceserver.PivotableResourceServerConfiguration;
import eu.solven.adhoc.tools.JdkUuidGenerator;

public class TestPivotableResourceServerConfiguration {
	PivotableResourceServerConfiguration conf = new PivotableResourceServerConfiguration();

	@Test
	public void testGenerateMultipleTimes() throws ParseException {
		MockEnvironment env = new MockEnvironment();
		env.setProperty(IPivotableOAuth2Constants.KEY_JWT_SIGNINGKEY, IPivotableOAuth2Constants.GENERATE);

		OctetSequenceKey key1 =
				PivotableResourceServerConfiguration.loadOAuth2SigningKey(env, JdkUuidGenerator.INSTANCE);
		OctetSequenceKey key2 =
				PivotableResourceServerConfiguration.loadOAuth2SigningKey(env, JdkUuidGenerator.INSTANCE);

		Assertions.assertThat(key1.toJSONString()).isEqualTo(key2.toJSONString());
	}
}
