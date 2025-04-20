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
package eu.solven.adhoc.table.composite;

import java.util.Map;
import java.util.concurrent.Phaser;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import eu.solven.adhoc.dag.context.ExecutingQueryContext;
import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.data.row.TabularRecordOverMaps;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.table.ITableWrapper;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * An {@link ITableWrapper} to be used to unittest for concurrency.
 * 
 * @author Benoit Lacelle
 */
@Builder
@Slf4j
public class PhasedTableWrapper implements ITableWrapper {
	@NonNull
	String name;

	@NonNull
	@Default
	@Getter
	TableWrapperPhasers phasers = TableWrapperPhasers.parties(0);

	@Value
	@Builder
	public static class TableWrapperPhasers {

		public static TableWrapperPhasers parties(int nbParties) {
			return TableWrapperPhasers.builder()
					.opening(new Phaser(nbParties))
					.streaming(new Phaser(nbParties))
					.closing(new Phaser(nbParties))
					.build();
		}

		// There is 3 phasers, one for each main step of the .streamSlices() operation
		// The idea is to move forward only when all parties (e.g. concurrent queries) get at given step.
		@NonNull
		@Default
		Phaser opening = new Phaser(0);

		@NonNull
		@Default
		Phaser streaming = new Phaser(0);

		@NonNull
		@Default
		Phaser closing = new Phaser(0);

		public void bulkRegister(int parties) {
			opening.bulkRegister(parties);
			streaming.bulkRegister(parties);
			closing.bulkRegister(parties);
		}

	}

	@Override
	public Map<String, Class<?>> getColumns() {
		return Map.of();
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public ITabularRecordStream streamSlices(ExecutingQueryContext executingQueryContext, TableQuery tableQuery) {
		log.info("opening arriveAndAwaitAdvance() {} {}", name, phasers.opening);
		phasers.opening.arriveAndAwaitAdvance();
		log.info("opening advance {}", name);

		return new ITabularRecordStream() {

			@Override
			public Stream<ITabularRecord> records() {
				log.info("streaming arriveAndAwaitAdvance() {} {}", name, phasers.streaming);
				phasers.streaming.arriveAndAwaitAdvance();
				log.info("streaming advance {}", name);

				Map<String, Object> slice = tableQuery.getGroupBy()
						.getNameToColumn()
						.keySet()
						.stream()
						.collect(Collectors.toMap(e -> e, e -> name));

				Map<String, Object> aggregates =
						tableQuery.getAggregators().stream().collect(Collectors.toMap(a -> a.getName(), a -> 1L));

				return Stream
						.<ITabularRecord>of(TabularRecordOverMaps.builder().slice(slice).aggregates(aggregates).build())
						.onClose(() -> this.close());
			}

			@Override
			public void close() {
				log.info("closing arriveAndAwaitAdvance() {} {}", name, phasers.closing);
				phasers.closing.arriveAndAwaitAdvance();
				log.info("closing advance {}", name);
			}
		};
	}

}
