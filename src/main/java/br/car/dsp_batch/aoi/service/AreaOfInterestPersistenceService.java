package br.car.dsp_batch.aoi.service;

import br.car.dsp_batch.aoi.config.AreaOfInterestConfig;
import br.car.dsp_batch.aoi.ddl.AreaOfInterestTableDdlBuilder;
import br.car.dsp_batch.aoi.metadata.AreaOfInterestTableMetadata;
import br.car.dsp_batch.geometry.GeometrySql;
import br.car.dsp_batch.layer.dto.LayerFeatureRecord;
import br.car.dsp_batch.layer.metadata.ColumnMetadata;
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

/**
 * Dual-write persistence for AOI: business target (bbox/centroid) + geo-target (full geometry).
 */
@Slf4j
@Service
public class AreaOfInterestPersistenceService {

    private final JdbcTemplate targetJdbcTemplate;
    private final JdbcTemplate geoTargetJdbcTemplate;

    public AreaOfInterestPersistenceService(
            @Qualifier("targetJdbcTemplate") JdbcTemplate targetJdbcTemplate,
            @Qualifier("geoTargetJdbcTemplate") JdbcTemplate geoTargetJdbcTemplate) {
        this.targetJdbcTemplate = targetJdbcTemplate;
        this.geoTargetJdbcTemplate = geoTargetJdbcTemplate;
    }

    public void upsertAll(List<LayerFeatureRecord> items, AreaOfInterestTableMetadata metadata) {
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
                log.warn("Skipping AOI record id={} due to null/empty geometry", item.getId());
            }
        }

        if (validItems.isEmpty()) {
            log.info(
                    "Upserted 0 AOI records into {} (skipped {} without geometry)",
                    metadata.qualifiedTargetTable(),
                    skippedWithoutGeometry
            );
            return;
        }

        upsertBusinessTarget(validItems, metadata);
        upsertGeoTarget(validItems, metadata);

        log.info(
                "Upserted {} AOI records into business + geo-target {} (skipped {} without geometry)",
                validItems.size(),
                metadata.qualifiedTargetTable(),
                skippedWithoutGeometry
        );
    }

    private void upsertBusinessTarget(List<LayerFeatureRecord> items,
                                      AreaOfInterestTableMetadata metadata) {
        List<String> targetColumns = metadata.targetNonGeometryColumnNames();
        String targetPk = metadata.resolveTargetPrimaryKeyColumn();

        Map<String, String> udtBySource = udtBySourceColumn(metadata);

        String insertColumns = String.join(", ", targetColumns)
                + ", " + AreaOfInterestTableDdlBuilder.BOUNDARY_BOX_COLUMN
                + ", " + AreaOfInterestTableDdlBuilder.CENTROID_COLUMN;
        String placeholders = targetColumns.stream().map(c -> "?").collect(Collectors.joining(", "));

        String updateSet = targetColumns.stream()
                .filter(col -> !col.equals(targetPk))
                .map(col -> col + " = EXCLUDED." + col)
                .collect(Collectors.joining(", "));
        String geoUpdate = AreaOfInterestTableDdlBuilder.BOUNDARY_BOX_COLUMN
                + " = EXCLUDED." + AreaOfInterestTableDdlBuilder.BOUNDARY_BOX_COLUMN
                + ", " + AreaOfInterestTableDdlBuilder.CENTROID_COLUMN
                + " = EXCLUDED." + AreaOfInterestTableDdlBuilder.CENTROID_COLUMN;
        updateSet = updateSet.isEmpty() ? geoUpdate : updateSet + ", " + geoUpdate;

        String sql = String.format(
                "INSERT INTO %s (%s) SELECT %s, %s ON CONFLICT (%s) DO UPDATE SET %s",
                metadata.qualifiedTargetTable(),
                insertColumns,
                placeholders,
                GeometrySql.envelopeAndCentroidFromEwkb(),
                targetPk,
                updateSet
        );

        try {
            targetJdbcTemplate.batchUpdate(sql, items, items.size(), (ps, item) -> {
                int index = 1;
                for (String targetColumn : targetColumns) {
                    index = bindAttribute(ps, index, item, metadata, targetColumn, udtBySource);
                }
                ps.setBytes(index, item.getGeometryEwkb());
            });
        } catch (DataAccessException e) {
            log.error("Critical failure during business-target AOI UPSERT for {}",
                    metadata.qualifiedTargetTable(), e);
            throw new RuntimeException(
                    "Error executing business-target AOI UPSERT for "
                            + metadata.qualifiedTargetTable() + ": " + e.getMessage(),
                    e);
        }
    }

    private void upsertGeoTarget(List<LayerFeatureRecord> items,
                                 AreaOfInterestTableMetadata metadata) {
        List<String> targetColumns = metadata.targetGeoNonGeometryColumnNames();
        String targetPk = metadata.resolveTargetPrimaryKeyColumn();
        String geom = metadata.resolveTargetGeometryColumn();

        Map<String, String> udtBySource = udtBySourceColumn(metadata);

        String insertColumns = String.join(", ", targetColumns) + ", " + geom;
        String placeholders = targetColumns.stream().map(c -> "?").collect(Collectors.joining(", "));
        String geometryPlaceholder = GeometrySql.multiGeomFromEwkbParam();

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
                    index = bindAttribute(ps, index, item, metadata, targetColumn, udtBySource);
                }
                ps.setBytes(index, item.getGeometryEwkb());
            });
        } catch (DataAccessException e) {
            log.error("Critical failure during geo-target AOI UPSERT for {}",
                    metadata.qualifiedTargetTable(), e);
            throw new RuntimeException(
                    "Error executing geo-target AOI UPSERT for "
                            + metadata.qualifiedTargetTable() + ": " + e.getMessage(),
                    e);
        }
    }

    private int bindAttribute(java.sql.PreparedStatement ps,
                              int index,
                              LayerFeatureRecord item,
                              AreaOfInterestTableMetadata metadata,
                              String targetColumn,
                              Map<String, String> udtBySource) throws java.sql.SQLException {
        String sourceColumn = metadata.resolveSourceColumnName(targetColumn);
        Object value = item.getAttribute(sourceColumn);
        if (AreaOfInterestConfig.CREATED_AT_COLUMN.equals(targetColumn)) {
            var instant = WatermarkTemporalBridge.toInstant(value, metadata.creationDateColumn());
            ps.setObject(index++, WatermarkTemporalBridge.toDspTimestamptz(instant));
            return index;
        }
        if (AreaOfInterestConfig.UPDATED_AT_COLUMN.equals(targetColumn)) {
            var instant = metadata.hasUpdatedAtColumn()
                    ? WatermarkTemporalBridge.toInstant(value, metadata.updatedAtColumn())
                    : null;
            ps.setObject(index++, WatermarkTemporalBridge.toDspTimestamptz(instant));
            return index;
        }
        if (AreaOfInterestConfig.ID_COLUMN.equals(targetColumn)
                || AreaOfInterestConfig.TERRITORY_LEVEL_3_ID_COLUMN.equals(targetColumn)) {
            ps.setString(index++, value == null ? null : String.valueOf(value));
            return index;
        }
        String udt = udtBySource.get(sourceColumn);
        if (udt != null && TemporalTypeClassifier.isTemporal(udt)) {
            CommonTemporalHandler.write(ps, index++, value, udt);
            return index;
        }
        ps.setObject(index++, value);
        return index;
    }

    private Map<String, String> udtBySourceColumn(AreaOfInterestTableMetadata metadata) {
        Map<String, String> udtBySource = new HashMap<>();
        for (ColumnMetadata column : metadata.columns()) {
            udtBySource.put(column.name(), column.udtName());
        }
        return udtBySource;
    }

    public static void requirePositiveSrid(AreaOfInterestTableMetadata metadata) {
        if (metadata.srid() <= 0) {
            throw new IllegalArgumentException(
                    "srid must be a positive integer for table " + metadata.qualifiedTargetTable()
                            + " (got " + metadata.srid() + ")");
        }
    }
}
