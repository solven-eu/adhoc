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
package eu.solven.adhoc.database.mongodb;

import java.util.Date;

import org.assertj.core.api.Assertions;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import de.flapdoodle.embed.mongo.commands.ServerAddress;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.mongo.transitions.ImmutableMongod;
import de.flapdoodle.embed.mongo.transitions.Mongod;
import de.flapdoodle.embed.mongo.transitions.RunningMongodProcess;
import de.flapdoodle.reverse.TransitionWalker;

public class TestEmbeddedMongo {

	protected TransitionWalker.ReachedState<RunningMongodProcess> running;
	protected ServerAddress serverAddress;

	@BeforeEach
	void startMongodb() {
		ImmutableMongod mongodConfig = Mongod.instance();
		Version.Main version = Version.Main.V8_1;

		running = mongodConfig.start(version);
		serverAddress = running.current().getServerAddress();
	}

	@AfterEach
	void teardownMongodb() {
		serverAddress = null;
		if (running != null) {
			running.close();
		}
		running = null;
	}

	// https://github.com/neelabalan/mongodb-sample-dataset/blob/main/sample_analytics/accounts.json
	// https://github.com/flapdoodle-oss/de.flapdoodle.embed.mongo/blob/main/docs/Howto.md
	@Test
	void testStuff() {
		try (MongoClient mongo = MongoClients.create("mongodb://" + serverAddress)) {
			MongoDatabase db = mongo.getDatabase("test");
			MongoCollection<Document> col = db.getCollection("testCol");
			col.insertOne(new Document("testDoc", new Date()));
			Assertions.assertThat(col.countDocuments()).isEqualTo(1L);
		}
	}
}