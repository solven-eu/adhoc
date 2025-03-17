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
package eu.solven.adhoc.app;

/**
 * The various Spring profiles used by this application.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IPivotableSpringProfiles {
	// Default logging configuration
	String P_LOGGING = "logging";

	// The default profile, activated when no other profile is defined. Typically useful for local runs.
	String P_DEFAULT = "default";
	// This will provide reasonable default for a fast+non_prod run
	String P_DEFAULT_SERVER = "default_server";
	// If true, we install at startup a small bunch of games
	String P_INJECT_DEFAULT_GAMES = "inject_default_games";

	// Activates the whole unsafe configuration
	String P_UNSAFE = "unsafe";
	// If true, we bypass the User login in the UI (i.e. the external-OAuth2 step required to produce account+player
	// tokens)
	String P_FAKEUSER = "fakeuser";
	// `fake_player` will enable relying on the fakePlayer but it will not tweat security related to
	// String P_FAKE_PLAYER = "fake_player";

	// The server hosting pivotable also host an adhoc endpoint
	String P_SELF_ENDPOINT = "self_endpoint";
	String P_SIMPLE_DATASETS = "simple_datasets";
	String P_ADVANCED_DATASETS = "advanced_datasets";

	// Provides unsafe security settings (like DEV OAuth2 providers, and an unsafe JWT signingKey)
	String P_UNSAFE_SERVER = "unsafe_server";
	// Provides unsafe JWT signingKey
	String P_UNSAFE_OAUTH2 = "unsafe_oauth2";
	// if true, we rely on external but unsafe OAuth2 Identity Providers
	String P_UNSAFE_EXTERNAL_OAUTH2 = "unsafe_external_oauth2";

	// Used when deployed on Heroku.
	String P_HEROKU = "heroku";

	// String P_SECURED = "secured";

	// Opposite to devmode. Should be activated in production
	// Checks there is not a single unsafe|fake configurations activated
	String P_PRDMODE = "prdmode";

	// InMemory enables easy run but lack of persistence
	String P_INMEMORY = "inmemory";
	// Redis will use some Redis persistence storage
	String P_REDIS = "redis";

	// Usable by the player-application
	String P_UNSAFE_PLAYER = "unsafe_player";
}