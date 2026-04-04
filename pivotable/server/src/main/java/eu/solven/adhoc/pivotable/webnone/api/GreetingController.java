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
package eu.solven.adhoc.pivotable.webnone.api;

import org.springframework.http.MediaType;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import eu.solven.adhoc.pivotable.api.IPivotableApiConstants;
import eu.solven.adhoc.pivotable.greeting.Greeting;
import lombok.AllArgsConstructor;

/**
 * Demonstrates controller, useful to do basic integrations tests before wiring real routes.
 * 
 * @author Benoit Lacelle
 */
@RestController
@AllArgsConstructor
public class GreetingController {

	@GetMapping(IPivotableApiConstants.PREFIX + "/public")
	public String publicEndpoint() {
		return "This is a public endpoint";
	}

	@GetMapping(IPivotableApiConstants.PREFIX + "/private")
	public String privateEndpoint(Authentication auth) {
		return "This is a private endpoint. hello:%s".formatted(auth.getName());
	}

	/**
	 * @return a static greeting message.
	 */
	@RequestMapping(produces = MediaType.APPLICATION_JSON_VALUE,
			path = IPivotableApiConstants.PREFIX + "/hello",
			method = { RequestMethod.GET, RequestMethod.POST })
	public Greeting hello() {
		return Greeting.builder().message("Hello, Spring!").build();
	}

}