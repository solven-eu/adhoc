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
package eu.solven.adhoc.security;

import java.util.ArrayList;
import java.util.List;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

import eu.solven.adhoc.engine.context.IImplicitFilter;
import eu.solven.adhoc.query.cube.ICubeQuery;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.FilterBuilder;
import eu.solven.adhoc.query.filter.ISliceFilter;
import lombok.extern.slf4j.Slf4j;

/**
 * An example {@link IAdhocImplicitFilter} based on Spring Security.
 */
@Slf4j
public class SpringSecurityAdhocImplicitFilter implements IImplicitFilter {
	public static final String ROLE_ADMIN = "ADMIN";
	public static final String ROLE_EUR = "EUR";

	@Override
	public ISliceFilter getImplicitFilter(ICubeQuery query) {
		final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
		return filterGivenAuth(authentication);
	}

	protected ISliceFilter filterGivenAuth(Authentication authentication) {
		if (authentication == null) {
			log.info("null authentication: matchNone");
			return ISliceFilter.MATCH_NONE;
		}

		List<ISliceFilter> operands = new ArrayList<>();

		// Some admin role
		if (authentication.getAuthorities().contains(new SimpleGrantedAuthority("ROLE_" + ROLE_ADMIN))) {
			operands.add(ISliceFilter.MATCH_ALL);
		}
		// Some role with static rules
		if (authentication.getAuthorities().contains(new SimpleGrantedAuthority("ROLE_" + ROLE_EUR))) {
			operands.add(ColumnFilter.equalTo("ccy", "EUR"));
		}
		// Some role with dynamic rules
		authentication.getAuthorities()
				.stream()
				.map(GrantedAuthority::getAuthority)
				.filter(ga -> ga.startsWith("ROLE_color="))
				.map(ga -> ga.substring("ROLE_color=".length()))
				.forEach(authorizedColor -> {
					operands.add(ColumnFilter.equalTo("color", authorizedColor));
				});

		// The user has access to the union of the authorized slices
		return FilterBuilder.or(operands).optimize();
	}
}
