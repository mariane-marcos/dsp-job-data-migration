package br.car.dsp_batch.layer.service;

import br.car.dsp_batch.geometry.GeometrySql;
import br.car.dsp_batch.layer.config.LayerConfig;
import br.car.dsp_batch.layer.dto.LayerFeatureRecord;
import br.car.dsp_batch.layer.metadata.ColumnMetadata;
import br.car.dsp_batch.layer.metadata.LayerTableMetadata;
import br.car.dsp_batch.temporal.CommonTemporalHandler;
import br.car.dsp_batch.temporal.TemporalTypeClassifier;
import br.car.dsp_batch.temporal.WatermarkTemporalBridge;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static br.car.dsp_batch.layer.config.LayerConfig.AREA_OF_INTEREST_ID_COLUMN;
import static br.car.dsp_batch.layer.config.LayerConfig.CREATED_AT_COLUMN;
import static br.car.dsp_batch.layer.config.LayerConfig.ID_COLUMN;
import static br.car.dsp_batch.layer.config.LayerConfig.UPDATED_AT_COLUMN;

/**
 * Persists layer features to the geo-target database (full geometry for WMS).
 */
@Slf4j
@Service
public class LayerFeaturePersistenceService {

    private final JdbcTemplate geoTargetJdbcTemplate;

    public LayerFeaturePersistenceService(
            @Qualifier("geoTargetJdbcTemplate") JdbcTemplate geoTargetJdbcTemplate) {
        this.geoTargetJdbcTemplate = geoTargetJdbcTemplate;
    }

    public void upsertAll(List<LayerFeatureRecord> items, LayerTableMetadata metadata) {
        if (items == null || items.isEmpty()) {
            return;
        }

        requirePositiveSrid(metadata);

        List<LayerFeatureRecord> validItems = new ArrayList<>();
        int skippedWithoutGeometry = 0;
        for (LayerFeatureRecord item : items) {
            if (item.getGeometryEwkb() != null && item.getGeometryEwkb().length > 0) {
                validItems.add(item);
            } else {
                skippedWithoutGeometry++;
                log.warn("Skipping feature id={} due to null/empty geometry", item.getId());
            }
        }

        if (validItems.isEmpty()) {
            log.info(
                    "Upserted 0 features into {} (skipped {} without geometry)",
                    metadata.qualifiedTargetTable(),
                    skippedWithoutGeometry
            );
            return;
        }

        upsertGeoTarget(validItems, metadata);
        log.info(
                "Upserted {} features into geo-target {} (skipped {} without geometry)",
                validItems.size(),
                metadata.qualifiedTargetTable(),
                skippedWithoutGeometry
        );
    }

    private void upsertGeoTarget(List<LayerFeatureRecord> items, LayerTableMetadata metadata) {
        List<String> targetColumns = metadata.targetNonGeometryColumnNames();
        String targetPk = metadata.resolveTargetPrimaryKeyColumn();
        String geom = metadata.resolveTargetGeometryColumn();

        Map<String, String> udtBySource = new HashMap<>();
        for (ColumnMetadata column : metadata.columns()) {
            udtBySource.put(column.name(), column.udtName());
        }

        String insertColumns = String.join(", ", targetColumns) + ", " + geom;
        String placeholders = targetColumns.stream().map(c -> "?").collect(Collectors.joining(", "));
        String geometryPlaceholder = GeometrySql.geomFromEwkbParam();

        String updateSet = targetColumns.stream()
                .filter(col -> !col.equals(targetPk))
                .map(col -> col + " = EXCLUDED." + col)
                .collect(Collectors.joining(", "));
        updateSet = updateSet.isEmpty()
                ? geom + " = EXCLUDED." + geom
                : updateSet + ", " + geom + " = EXCLUDED." + geom;

        String sql = String.format(
                "INSERT INTO %s (%s) VALUES (%s, %s) ON CONFLICT (%s) DO UPDATE SET %s",
                metadata.qualifiedTargetTable(),
                insertColumns,
                placeholders,
                geometryPlaceholder,
                targetPk,
                updateSet
        );

        try {
            geoTargetJdbcTemplate.batchUpdate(sql, items, items.size(), (ps, item) -> {
                int index = 1;
                for (String targetColumn : targetColumns) {
                    String sourceColumn = metadata.resolveSourceColumnName(targetColumn);
                    Object value = item.getAttribute(sourceColumn);
                    if (CREATED_AT_COLUMN.equals(targetColumn)) {
                        var instant = WatermarkTemporalBridge.toInstant(
                                value, metadata.creationDateColumn());
                        ps.setObject(index++, WatermarkTemporalBridge.toDspTimestamptz(instant));
                    } else if (UPDATED_AT_COLUMN.equals(targetColumn)) {
                        var instant = metadata.hasUpdatedAtColumn()
                                ? WatermarkTemporalBridge.toInstant(value, metadata.updatedAtColumn())
                                : null;
                        ps.setObject(index++, WatermarkTemporalBridge.toDspTimestamptz(instant));
                    } else if (ID_COLUMN.equals(targetColumn)
                            || AREA_OF_INTEREST_ID_COLUMN.equals(targetColumn)) {
                        ps.setString(index++, value == null ? null : String.valueOf(value));
                    } else {
                        String udt = udtBySource.get(sourceColumn);
                        if (udt != null && TemporalTypeClassifier.isTemporal(udt)) {
                            CommonTemporalHandler.write(ps, index++, value, udt);
                        } else {
                            ps.setObject(index++, value);
                        }
                    }
                }
                ps.setBytes(index, item.getGeometryEwkb());
            });
        } catch (DataAccessException e) {
            log.error("Critical failure during geo-target UPSERT for {}",
                    metadata.qualifiedTargetTable(), e);
            throw new RuntimeException(
                    "Error executing geo-target UPSERT for "
                            + metadata.qualifiedTargetTable() + ": " + e.getMessage(),
                    e);
        }
    }

    public static void requirePositiveSrid(LayerTableMetadata metadata) {
        if (metadata.srid() <= 0) {
            throw new IllegalArgumentException(
                    "srid must be a positive integer for table " + metadata.qualifiedTargetTable()
                            + " (got " + metadata.srid() + ")");
        }
    }
}
