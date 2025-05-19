package eu.solven.adhoc.atoti.migration;

import eu.solven.adhoc.resource.MeasureForestFromResource;
import eu.solven.adhoc.resource.MeasureForests;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.FileSystemResource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A {@link AConvertToAdhoc} which will persist each cube forest into an individual file.
 */
@SuperBuilder
@Slf4j
public class ConvertToAdhocInPath extends AConvertToAdhoc {

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
    MeasureForestFromResource measuresSetFromResource = new MeasureForestFromResource();

    protected Path getPathForPivot(String pivotId) {
        return directory.resolve("forest-" + pivotId + "." + format);
    }

    @Override
    protected void onForests(MeasureForests forests) {
        forests.getForests().forEach(forest -> {
            String pivotId = forest.getName();
            Path pathForPivot = getPathForPivot(pivotId);
            try {
                String asString = measuresSetFromResource.asString(format, forest);

                log.info("Writing -> measureForest for {}: length={}", pivotId, asString.length());
                Files.writeString(pathForPivot, asString);
                log.info("Written <- measureForest for {}: length={}", pivotId, asString.length());

                try {
                    // Check we can load back the configuration file
                    // TODO This may be valid only for local file-systems
                    measuresSetFromResource.loadForestFromResource(pivotId, format, new FileSystemResource(pathForPivot));
                } catch (Throwable e) {
                    log.warn("Issue loading-back path={} from adhoc for {}", pathForPivot, pivotId, e);
                }
            } catch (Throwable e) {
                log.warn("Issue converting to adhoc for {}", pivotId, e);
            }
        });

        log.info("Written Adhoc files into {}", directory);
    }

    public static ConvertToAdhocInPathBuilder<?,?> openForTemporaryFolder() throws IOException {
        Path directory = Files.createTempDirectory(ConvertToAdhocInPath.class.getSimpleName());
        return ConvertToAdhocInPath.builder().directory(directory);
    }
}
