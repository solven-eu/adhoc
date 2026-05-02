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
package eu.solven.adhoc.jol;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import lombok.extern.slf4j.Slf4j;

/**
 * JUnit 5 extension that suppresses the JOL HotSpot SA attach warning before any test in the class runs.
 *
 * <p>
 * Without this, JOL attempts to attach to the HotSpot SA (serviceability agent) to gather layout information, which
 * triggers a warning or failure in CI environments that do not allow ptrace. Setting
 * {@code jol.skipHotspotSAAttach=true} tells JOL to skip that step.
 *
 * <p>
 * Usage:
 *
 * <pre>{@code
 * &#64;ExtendWith(JolExtension.class)
 * class MyTest { ... }
 * }</pre>
 *
 * @see <a href="https://bugs.openjdk.org/browse/CODETOOLS-7903447">CODETOOLS-7903447</a>
 */
@Slf4j
public class JolExtension implements BeforeAllCallback {

	@Override
	public void beforeAll(ExtensionContext context) {
		log.info("Setting {} to {}", "jol.skipHotspotSAAttach", "true");
		System.setProperty("jol.skipHotspotSAAttach", "true");
	}
}
