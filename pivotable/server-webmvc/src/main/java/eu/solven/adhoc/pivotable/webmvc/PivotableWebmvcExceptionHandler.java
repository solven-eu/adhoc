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
package eu.solven.adhoc.pivotable.webmvc;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import eu.solven.adhoc.pivotable.security.AccountForbiddenOperation;
import eu.solven.adhoc.pivotable.security.LoginRouteButNotAuthenticatedException;
import eu.solven.pepper.system.PepperEnvHelper;
import lombok.extern.slf4j.Slf4j;

/**
 * Converts applicative exceptions into appropriate HTTP status codes, mirroring the behaviour of the WebFlux
 * {@code PivotableWebExceptionHandler}.
 *
 * @author Benoit Lacelle
 */
@SuppressWarnings("checkstyle:MagicNumber")
@RestControllerAdvice
@Slf4j
public class PivotableWebmvcExceptionHandler {

	/**
	 * @param e
	 *            the unauthenticated-access exception
	 * @return 401 Unauthorized
	 */
	@ExceptionHandler(LoginRouteButNotAuthenticatedException.class)
	public ResponseEntity<Map<String, Object>> handleLoginRequired(LoginRouteButNotAuthenticatedException e) {
		return respond(HttpStatus.UNAUTHORIZED, e);
	}

	/**
	 * @param e
	 *            the forbidden-operation exception
	 * @return 403 Forbidden
	 */
	@ExceptionHandler(AccountForbiddenOperation.class)
	public ResponseEntity<Map<String, Object>> handleForbidden(AccountForbiddenOperation e) {
		return respond(HttpStatus.FORBIDDEN, e);
	}

	/**
	 * @param e
	 *            an illegal argument exception
	 * @return 400 Bad Request
	 */
	@ExceptionHandler(IllegalArgumentException.class)
	public ResponseEntity<Map<String, Object>> handleBadRequest(IllegalArgumentException e) {
		return respond(HttpStatus.BAD_REQUEST, e);
	}

	private ResponseEntity<Map<String, Object>> respond(HttpStatus status, Throwable e) {
		if (log.isDebugEnabled() || PepperEnvHelper.inUnitTest()) {
			log.warn("Returning a {} given {} ({})", status, e.getClass(), e.getMessage(), e);
		} else {
			log.warn("Returning a {} given {} ({})", status, e.getClass(), e.getMessage());
		}

		Map<String, Object> body = new LinkedHashMap<>();
		body.put("error_message", Optional.ofNullable(e.getMessage()).orElse(""));
		return ResponseEntity.status(status).body(body);
	}

}
