package br.car.dsp_batch.sync;

import br.car.dsp_batch.geometry.GeometrySql;
import br.car.dsp_batch.temporal.TemporalType;
import br.car.dsp_batch.temporal.WatermarkTemporalBridge;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Optional;

/**
 * Shared engine for incremental watermark change detection (delta only).
 * Orphan reconciliation runs in dedicated jobs ({@link br.car.dsp_batch.reconciliation.ReconciliationJobRunner}).
 */
@Slf4j
@Service
public class WatermarkChangeDetectionEngine {

    private final SyncStateRepository syncStateRepository;
    private final CycleAffectedExtent cycleAffectedExtent;

    public WatermarkChangeDetectionEngine(SyncStateRepository syncStateRepository,
                                          CycleAffectedExtent cycleAffectedExtent) {
        this.syncStateRepository = syncStateRepository;
        this.cycleAffectedExtent = cycleAffectedExtent;
    }

    public void detectChanges(JdbcTemplate sourceJdbc,
                              JdbcTemplate geoTargetJdbc,
                              JdbcTemplate businessTargetJdbc,
                              WatermarkTableSpec spec,
                              ChunkContext chunkContext) {
        requireSpec(spec);
        log.info("Starting change detection for table={} syncKey={}",
                spec.sourceTable(), spec.syncKey());
        logDateGranularityWarnings(spec);

        Optional<SyncState> state = syncStateRepository.findBySyncKey(spec.syncKey());
        Instant watermark = state.map(SyncState::watermarkLastEventAt).orElse(null);

        DeltaAggregate delta = collectDelta(sourceJdbc, spec, watermark);
        if (delta == null) {
            delta = new DeltaAggregate(0, null, null);
        }

        var jobContext = chunkContext.getStepContext()
                .getStepExecution()
                .getJobExecution()
                .getExecutionContext();

        jobContext.putString(WatermarkContextKeys.SYNC_KEY, spec.syncKey());
        jobContext.putString(WatermarkContextKeys.SOURCE_TABLE, spec.sourceTable());

        if (watermark != null) {
            jobContext.putString(WatermarkContextKeys.PREVIOUS_WATERMARK, watermark.toString());
        }

        if (delta.maxEventAt() != null) {
            jobContext.putString(WatermarkContextKeys.PROPOSED_WATERMARK, delta.maxEventAt().toString());
        }

        cycleAffectedExtent.replace(spec.syncKey(), delta.extent());
        if (delta.count() == 0) {
            log.info("No changes detected in {} (watermark={})", spec.sourceTable(), watermark);
            jobContext.put(WatermarkContextKeys.HAS_CHANGES, false);
            return;
        }

        log.info("Detected {} changed record(s) in {} (watermark={}, proposed={}, extent={})",
                delta.count(), spec.sourceTable(), watermark, delta.maxEventAt(), delta.extent());
        jobContext.put(WatermarkContextKeys.HAS_CHANGES, true);
        if (delta.extent() != null) {
            jobContext.putString(WatermarkContextKeys.AFFECTED_EXTENT, delta.extent());
        }
        if (spec.layerName() != null) {
            jobContext.putString(WatermarkContextKeys.LAYER_NAME, spec.layerName());
        }
    }

    DeltaAggregate collectDelta(JdbcTemplate sourceJdbc, WatermarkTableSpec spec, Instant watermark) {
        String sql = deltaAggregateSql(spec, watermark);
        DeltaAggregate delta = sourceJdbc.query(sql, (ResultSetExtractor<DeltaAggregate>) rs -> readDelta(rs, spec));
        if (delta == null) {
            return new DeltaAggregate(0, null, null);
        }
        log.info("Source delta: {} record(s) after watermark {}", delta.count(), watermark);
        return delta;
    }

    static String deltaAggregateSql(WatermarkTableSpec spec, Instant watermark) {
        String geom = spec.sourceGeometryColumn();
        String creationDate = spec.sourceCreationDateColumn();
        String updatedAt = spec.sourceUpdatedAtColumn();
        String table = spec.sourceTable();

        String validGeomFilter = GeometrySql.validNonEmptyPredicate(geom);
        String configWhere = spec.whereClause();
        boolean hasConfigWhere = configWhere != null
                && !configWhere.isBlank()
                && !"1=1".equals(configWhere.trim());

        StringBuilder where = new StringBuilder("WHERE ").append(validGeomFilter);
        String changeFilter = WatermarkSql.buildChangeDetectionFilter(
                spec.creationDateColumn(), spec.updatedAtColumn(), watermark);
        where.append(" AND ").append(changeFilter);
        if (hasConfigWhere) {
            where.append(" AND (").append(configWhere).append(")");
        }

        String geom3857 = GeometrySql.inWebMercator(geom, spec.srid());
        String innerColumns = creationDate + " AS feature_created_at, " + geom3857 + " AS g3857";
        String aggregateColumns = "COUNT(*) AS delta_count, MAX(feature_created_at) AS max_created_at, "
                + "ST_Extent(g3857) AS ext";
        String outerColumns = "delta_count, max_created_at, "
                + "ST_XMin(ext) AS minx, ST_YMin(ext) AS miny, ST_XMax(ext) AS maxx, ST_YMax(ext) AS maxy";
        if (updatedAt != null) {
            innerColumns = creationDate + " AS feature_created_at, " + updatedAt
                    + " AS feature_updated_at, " + geom3857 + " AS g3857";
            aggregateColumns = "COUNT(*) AS delta_count, MAX(feature_created_at) AS max_created_at, "
                    + "MAX(feature_updated_at) AS max_updated_at, ST_Extent(g3857) AS ext";
            outerColumns = "delta_count, max_created_at, max_updated_at, "
                    + "ST_XMin(ext) AS minx, ST_YMin(ext) AS miny, ST_XMax(ext) AS maxx, ST_YMax(ext) AS maxy";
        }

        return "SELECT " + outerColumns
                + " FROM (SELECT " + aggregateColumns
                + " FROM (SELECT " + innerColumns
                + " FROM " + table + " " + where
                + ") delta) agg";
    }

    private static DeltaAggregate readDelta(ResultSet rs, WatermarkTableSpec spec) throws SQLException {
        if (!rs.next()) {
            return new DeltaAggregate(0, null, null);
        }
        long count = rs.getLong("delta_count");
        Instant creationInstant = WatermarkTemporalBridge.readInstant(
                rs, "max_created_at", spec.creationDateColumn());
        Instant updatedInstant = spec.sourceUpdatedAtColumn() == null
                ? null
                : WatermarkTemporalBridge.readInstant(rs, "max_updated_at", spec.updatedAtColumn());
        Instant maxEventAt = WatermarkTemporalBridge.maxEventInstant(creationInstant, updatedInstant);

        String extent = null;
        double minX = rs.getDouble("minx");
        if (!rs.wasNull()) {
            extent = AffectedExtent.format(minX, rs.getDouble("miny"), rs.getDouble("maxx"), rs.getDouble("maxy"));
        }
        return new DeltaAggregate(count, maxEventAt, extent);
    }

    private void logDateGranularityWarnings(WatermarkTableSpec spec) {
        if (spec.creationDateColumn().sourceType() == TemporalType.DATE) {
            log.warn(
                    "creation-date-column '{}' on {} is DATE — watermark granularity is daily "
                            + "(source-timezone={})",
                    spec.sourceCreationDateColumn(),
                    spec.sourceTable(),
                    spec.creationDateColumn().policy().zoneIdOrNull());
        }
        if (spec.hasUpdatedAtColumn()
                && spec.updatedAtColumn().sourceType() == TemporalType.DATE) {
            log.warn(
                    "updated-at-column '{}' on {} is DATE — watermark granularity is daily "
                            + "(source-timezone={})",
                    spec.sourceUpdatedAtColumn(),
                    spec.sourceTable(),
                    spec.updatedAtColumn().policy().zoneIdOrNull());
        }
    }

    private static void requireSpec(WatermarkTableSpec spec) {
        if (spec == null) {
            throw new IllegalArgumentException("WatermarkTableSpec is required");
        }
        if (spec.syncKey() == null || spec.syncKey().isBlank()) {
            throw new IllegalArgumentException("WATERMARK requires sync-key");
        }
        if (spec.creationDateColumn() == null) {
            throw new IllegalArgumentException(
                    "WATERMARK requires creation-date-column for table " + spec.sourceTable());
        }
    }

    record DeltaAggregate(long count, Instant maxEventAt, String extent) {
    }
}
