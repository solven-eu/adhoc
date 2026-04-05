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
package eu.solven.adhoc.pivotable.webmvc.api;

import java.util.Map;

import org.springframework.context.ApplicationContext;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import eu.solven.adhoc.pivotable.api.IPivotableApiConstants;
import eu.solven.adhoc.util.IHasCache;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * API enabling clearing anything transient. Useful for integration tests.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
@RestController
@RequestMapping(IPivotableApiConstants.PREFIX)
public class PivotableClearController {
	final ApplicationContext appContext;

	// SPRING_CSRF_UNRESTRICTED_REQUEST_MAPPING seems a false-positive as we have `method = { RequestMethod.GET,
	// RequestMethod.POST }`
	@SuppressFBWarnings(value = "SPRING_CSRF_UNRESTRICTED_REQUEST_MAPPING",
			justification = "This endpoint is on the JWT resource-server filter chain which disables CSRF (see PivotableJwtWebmvcSecurity.onCsrf).")
	@RequestMapping(method = { RequestMethod.GET, RequestMethod.POST }, path = "/clear")
	public Map<String, ?> clear() {
		Map<String, IHasCache> beansWithCache = appContext.getBeansOfType(IHasCache.class);

		beansWithCache.forEach((name, bean) -> {
			log.info("About to IHasCache.invalidateAll() over bean={} ({})", name, bean);
			bean.invalidateAll();
		});

		return Map.of("clear", true, "caches", beansWithCache.keySet());
	}
}
