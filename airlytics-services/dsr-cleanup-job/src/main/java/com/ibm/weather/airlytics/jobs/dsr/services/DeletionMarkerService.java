package com.ibm.weather.airlytics.jobs.dsr.services;

import com.ibm.weather.airlytics.common.airlock.AirlockException;
import com.ibm.weather.airlytics.jobs.dsr.dto.DeletedUserMarker;
import com.ibm.weather.airlytics.jobs.dsr.dto.DsrJobAirlockConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class DeletionMarkerService {

    private static final Logger logger = LoggerFactory.getLogger(DeletionMarkerService.class);

    public List<DeletedUserMarker> listCurrentMarkersForShard(DsrJobAirlockConfig featureConfig, int shard) throws AirlockException {
        String shardPath = featureConfig.getMarkersBaseFolder();

        if(!shardPath.endsWith("/")) {
            shardPath = shardPath + "/";
        }
        shardPath = shardPath + "shard=" + shard + "/";
        Path path = Paths.get(shardPath);

        if(path.toFile().exists()) {
            logger.info("Scanning {}", shardPath);

            try (Stream<Path> walk = Files.walk(path)) {

                List<DeletedUserMarker> result = walk
                        .filter(Files::isRegularFile)
                        .filter(p -> !p.getFileName().toString().startsWith("."))
                        .filter(p -> p.toString().contains("shard=" + shard + "/day="))
                        .map(p -> DeletedUserMarker.fromMarkerPath(p))
                        .collect(Collectors.toList());

                if (!result.isEmpty()) {
                    logger.info("Found {} markers in shard {}", result.size(), shard);
                }
                return result;
            } catch (IOException e) {
                throw new AirlockException("Error reading deletion markers", e);
            }
        }
        return Collections.emptyList();
    }

    public void archiveProcessedMarkersForShard(DsrJobAirlockConfig featureConfig, List<DeletedUserMarker> markers, int shard)  throws AirlockException {

        try {
            Path destFolder = Paths.get(featureConfig.getMarkersArchiveFolder());

            for(DeletedUserMarker m : markers) {

                if(m.getShard() == shard) {
                    String mPath = m.getMarkerPath().toString();
                    int idx = mPath.indexOf(featureConfig.getArchivedPartAfter());
                    if(idx < 0) {
                        throw new AirlockException(mPath + " does not contain archived part marker " + featureConfig.getArchivedPartAfter());
                    }
                    idx += featureConfig.getArchivedPartAfter().length();
                    String movedPart = mPath.substring(idx);

                    if(movedPart.startsWith("/")) {
                        movedPart = movedPart.substring(1);
                    }
                    Path destPath = destFolder.resolve(movedPart);
                    destPath.getParent().toFile().mkdirs();
                    logger.info("Archiving {} to {}", m.getMarkerPath(), destPath);
                    Files.move(m.getMarkerPath(), destPath, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        } catch (IOException e) {
            throw new AirlockException("Error archiving deletion markers", e);
        }

    }
}
