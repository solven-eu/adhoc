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
package eu.solven.adhoc.atoti.translation;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.springframework.core.io.FileSystemResource;

import eu.solven.adhoc.resource.MeasureForestFromResource;
import eu.solven.adhoc.resource.MeasureForests;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link ATranslateToAdhoc} which will persist each cube forest into an individual file.
 */
@SuperBuilder
@Slf4j
public class TranslateToAdhocInPath extends ATranslateToAdhoc {

	@NonNull
	@Getter
	Path directory;

	// json or yml
	@NonNull
	@Builder.Default
	@Getter
	String format = "json";

	@NonNull
	@Builder.Default
	MeasureForestFromResource fromResource = new MeasureForestFromResource();

	protected Path getPathForPivot(String pivotId) {
		return directory.resolve("forest-" + pivotId + "." + format);
	}

	@Override
	protected void onForests(MeasureForests forests) {
		forests.getForests().forEach(forest -> {
			String pivotId = forest.getName();
			Path pathForPivot = getPathForPivot(pivotId);
			try {
				String asString = fromResource.asString(format, forest);

				log.info("Writing -> measureForest for {}: length={}", pivotId, asString.length());
				Files.writeString(pathForPivot, asString);
				log.info("Written <- measureForest for {}: length={}", pivotId, asString.length());

				try {
					// Check we can load back the configuration file
					// TODO This may be valid only for local file-systems
					fromResource.loadForestFromResource(pivotId, format, new FileSystemResource(pathForPivot));
				} catch (Throwable e) {
					log.warn("Issue loading-back path={} from adhoc for {}", pathForPivot, pivotId, e);
				}
			} catch (Throwable e) {
				log.warn("Issue converting to adhoc for {}", pivotId, e);
			}
		});

		log.info("Written Adhoc files into {}", directory);
	}

	public static TranslateToAdhocInPathBuilder<?, ?> openForTemporaryFolder() throws IOException {
		Path directory = Files.createTempDirectory(TranslateToAdhocInPath.class.getSimpleName());
		return TranslateToAdhocInPath.builder().directory(directory);
	}
}
