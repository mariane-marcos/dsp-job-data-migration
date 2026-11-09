package br.car.dsp_batch.service;

import br.car.dsp_batch.batch.config.JobTableConfig;
import br.car.dsp_batch.batch.dto.AdministrativeUnitDTO;
import br.car.dsp_batch.geometry.GeometrySql;
import br.car.dsp_batch.temporal.CommonTemporalHandler;
import br.car.dsp_batch.temporal.TemporalTypeClassifier;
import br.car.dsp_batch.temporal.TemporalColumnSpecs;
import br.car.dsp_batch.temporal.WatermarkColumnSpec;
import br.car.dsp_batch.temporal.WatermarkTemporalBridge;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Dual-write persistence: business target (bbox/centroid) + geo target (full geometry).
 * Fail-fast: any destination failure aborts the chunk; re-run is idempotent via UPSERT.
 */
@Slf4j
@Service
public class AdministrativeUnitPersistenceService {

    private static final String BOUNDARY_BOX_COLUMN = "boundary_box";
    private static final String CENTROID_COLUMN = "centroid_coordinates";

    private final JdbcTemplate targetJdbcTemplate;
    private final JdbcTemplate geoTargetJdbcTemplate;

    public AdministrativeUnitPersistenceService(
            @Qualifier("targetJdbcTemplate") JdbcTemplate targetJdbcTemplate,
            @Qualifier("geoTargetJdbcTemplate") JdbcTemplate geoTargetJdbcTemplate) {
        this.targetJdbcTemplate = targetJdbcTemplate;
        this.geoTargetJdbcTemplate = geoTargetJdbcTemplate;
    }

    public void upsertAll(List<AdministrativeUnitDTO> items, JobTableConfig tableConfig) {
        upsertAll(items, tableConfig, null);
    }

    public void upsertAll(List<AdministrativeUnitDTO> items,
                          JobTableConfig tableConfig,
                          TemporalColumnSpecs temporalColumns) {
        if (items == null || items.isEmpty()) {
            return;
        }

        requirePositiveSrid(tableConfig);

        List<AdministrativeUnitDTO> validItems = new ArrayList<>();
        int skippedWithoutGeometry = 0;
        for (AdministrativeUnitDTO item : items) {
            if (item.getGeometryEwkb() != null && item.getGeometryEwkb().length > 0) {
                validItems.add(item);
            } else {
                skippedWithoutGeometry++;
                log.warn("Skipping record id={} due to null/empty geometry", item.getId());
            }
        }

        if (validItems.isEmpty()) {
            log.info(
                    "Upserted 0 records into {} / geo-target (skipped {} without geometry)",
                    tableConfig.getTargetTable(),
                    skippedWithoutGeometry
            );
            return;
        }

        upsertBusinessTarget(validItems, tableConfig, temporalColumns);
        upsertGeoTarget(validItems, tableConfig, temporalColumns);

        log.info(
                "Upserted {} records into business target {} and geo-target (skipped {} without geometry)",
                validItems.size(),
                tableConfig.getTargetTable(),
                skippedWithoutGeometry
        );
    }

    private void upsertBusinessTarget(List<AdministrativeUnitDTO> items,
                                      JobTableConfig tableConfig,
                                      TemporalColumnSpecs temporalColumns) {
        List<String> sourceColumns = tableConfig.getPersistColumns();
        List<String> targetColumns = sourceColumns.stream()
                .map(tableConfig::resolveTargetColumn)
                .collect(Collectors.toList());
        String targetPk = tableConfig.resolveTargetColumn(tableConfig.getPrimaryKey());

        String insertColumns = String.join(", ", targetColumns)
                + ", " + BOUNDARY_BOX_COLUMN
                + ", " + CENTROID_COLUMN;
        String placeholders = sourceColumns.stream()
                .map(c -> "?")
                .collect(Collectors.joining(", "));

        String updateSet = targetColumns.stream()
                .filter(col -> !col.equals(targetPk))
                .map(col -> col + " = EXCLUDED." + col)
                .collect(Collectors.joining(", "));
        String geoUpdate = BOUNDARY_BOX_COLUMN + " = EXCLUDED." + BOUNDARY_BOX_COLUMN
                + ", " + CENTROID_COLUMN + " = EXCLUDED." + CENTROID_COLUMN;
        updateSet = updateSet.isEmpty() ? geoUpdate : updateSet + ", " + geoUpdate;

        String sql = String.format(
                "INSERT INTO %s (%s) SELECT %s, %s ON CONFLICT (%s) DO UPDATE SET %s",
                tableConfig.getTargetTable(),
                insertColumns,
                placeholders,
                GeometrySql.envelopeAndCentroidFromEwkb(),
                targetPk,
                updateSet
        );

        try {
            targetJdbcTemplate.batchUpdate(sql, items, items.size(), (ps, item) -> {
                int index = 1;
                for (String column : sourceColumns) {
                    index = bindAttribute(ps, index, item, column, tableConfig, temporalColumns);
                }
                ps.setBytes(index, item.getGeometryEwkb());
            });
        } catch (DataAccessException e) {
            log.error("Critical failure during business-target UPSERT for {}",
                    tableConfig.getTargetTable(), e);
            throw new RuntimeException(
                    "Error executing business-target UPSERT for "
                            + tableConfig.getTargetTable() + ": " + e.getMessage(),
                    e);
        }
    }

    private void upsertGeoTarget(List<AdministrativeUnitDTO> items,
                                 JobTableConfig tableConfig,
                                 TemporalColumnSpecs temporalColumns) {
        List<String> sourceColumns = tableConfig.getPersistColumns();
        List<String> targetColumns = sourceColumns.stream()
                .map(tableConfig::resolveTargetColumn)
                .collect(Collectors.toList());
        String targetGeom = tableConfig.resolveTargetColumn(tableConfig.getGeometryColumn());
        String targetPk = tableConfig.resolveTargetColumn(tableConfig.getPrimaryKey());

        String insertColumns = String.join(", ", targetColumns) + ", " + targetGeom;
        String placeholders = sourceColumns.stream()
                .map(c -> "?")
                .collect(Collectors.joining(", "));
        String geometryPlaceholder = GeometrySql.multiGeomFromEwkbParam();

        String updateSet = targetColumns.stream()
                .filter(col -> !col.equals(targetPk))
                .map(col -> col + " = EXCLUDED." + col)
                .collect(Collectors.joining(", "));
        updateSet = updateSet.isEmpty()
                ? targetGeom + " = EXCLUDED." + targetGeom
                : updateSet + ", " + targetGeom + " = EXCLUDED." + targetGeom;

        String sql = String.format(
                "INSERT INTO %s (%s) VALUES (%s, %s) ON CONFLICT (%s) DO UPDATE SET %s",
                tableConfig.getTargetTable(),
                insertColumns,
                placeholders,
                geometryPlaceholder,
                targetPk,
                updateSet
        );

        try {
            geoTargetJdbcTemplate.batchUpdate(sql, items, items.size(), (ps, item) -> {
                int index = 1;
                for (String column : sourceColumns) {
                    index = bindAttribute(ps, index, item, column, tableConfig, temporalColumns);
                }
                ps.setBytes(index, item.getGeometryEwkb());
            });
        } catch (DataAccessException e) {
            log.error("Critical failure during geo-target UPSERT for {}",
                    tableConfig.getTargetTable(), e);
            throw new RuntimeException(
                    "Error executing geo-target UPSERT for "
                            + tableConfig.getTargetTable() + ": " + e.getMessage(),
                    e);
        }
    }

    private int bindAttribute(PreparedStatement ps,
                              int index,
                              AdministrativeUnitDTO item,
                              String sourceColumn,
                              JobTableConfig tableConfig,
                              TemporalColumnSpecs temporalColumns) throws SQLException {
        Object value = item.getAttribute(sourceColumn);
        if (temporalColumns != null) {
            WatermarkColumnSpec spec = temporalColumns.specForSourceColumn(sourceColumn);
            if (spec != null) {
                var instant = WatermarkTemporalBridge.toInstant(value, spec);
                ps.setObject(index, WatermarkTemporalBridge.toDspTimestamptz(instant));
                return index + 1;
            }
        }
        if (value != null && TemporalTypeClassifier.isTemporal(guessUdtFromValue(value))) {
            CommonTemporalHandler.write(
                    ps, index, value, TemporalTypeClassifier.classify(guessUdtFromValue(value)));
            return index + 1;
        }
        ps.setObject(index, value);
        return index + 1;
    }

    private static String guessUdtFromValue(Object value) {
        if (value instanceof java.time.LocalDate) {
            return "date";
        }
        if (value instanceof java.time.LocalDateTime) {
            return "timestamp";
        }
        if (value instanceof java.time.OffsetDateTime) {
            return "timestamptz";
        }
        if (value instanceof java.time.LocalTime) {
            return "time";
        }
        if (value instanceof java.time.OffsetTime) {
            return "timetz";
        }
        return value.getClass().getSimpleName().toLowerCase(Locale.ROOT);
    }

    public static void requirePositiveSrid(JobTableConfig tableConfig) {
        if (tableConfig.getSrid() <= 0) {
            throw new IllegalArgumentException(
                    "srid must be a positive integer in the job YAML for table "
                            + tableConfig.getTargetTable()
                            + " (got " + tableConfig.getSrid() + ")");
        }
    }
}
