package br.car.dsp_batch.reconciliation;

import br.car.dsp_batch.aoi.config.AreaOfInterestConfig;
import br.car.dsp_batch.kpi.ddl.KpiMeasureTableDdlBuilder;
import br.car.dsp_batch.layer.config.LayerConfig;
import br.car.dsp_batch.layer.config.LayersProperties;
import br.car.dsp_batch.layer.introspection.SchemaIntrospectionService;
import br.car.dsp_batch.layer.metadata.QualifiedTable;
import br.car.dsp_batch.sync.AffectedExtent;
import br.car.dsp_batch.sync.DepartedLevel3ContextSupport;
import br.car.dsp_batch.sync.SyncKeys;
import br.car.dsp_batch.sync.WatermarkContextKeys;
import br.car.dsp_batch.sync.WatermarkTableSpec;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Deletes geo-target rows whose primary keys no longer exist on the source (orphans).
 * Invoked in batches by reconciliation workers.
 */
@Slf4j
@Service
public class OrphanReconciliationService {

    private final LayersProperties layersProperties;
    private final SchemaIntrospectionService schemaIntrospectionService;
    private final ReconciliationProperties reconciliationProperties;

    public OrphanReconciliationService(ReconciliationProperties reconciliationProperties) {
        this(reconciliationProperties, null, null);
    }

    @Autowired
    public OrphanReconciliationService(ReconciliationProperties reconciliationProperties,
                                       LayersProperties layersProperties,
                                       SchemaIntrospectionService schemaIntrospectionService) {
        this.reconciliationProperties = reconciliationProperties;
        this.layersProperties = layersProperties;
        this.schemaIntrospectionService = schemaIntrospectionService;
    }

    /**
     * Removes orphan PKs and unions {@code orphanExtent} into the job's single extent.
     */
    public void reconcileOrphans(JdbcTemplate geoTargetJdbc,
                                 JdbcTemplate businessTargetJdbc,
                                 WatermarkTableSpec spec,
                                 Collection<?> orphanIds,
                                 String orphanExtent,
                                 ExecutionContext jobContext) {
        if (orphanIds == null || orphanIds.isEmpty()) {
            return;
        }

        Set<Object> orphans = new HashSet<>(orphanIds);
        for (Object id : orphans) {
            log.warn("DELETED orphan: id={}", id);
        }

        captureDepartedTerritoriesFromOrphans(geoTargetJdbc, spec, orphans, jobContext);
        deleteOrphanChildrenInBatches(businessTargetJdbc, geoTargetJdbc, spec, orphans);

        if (businessTargetJdbc != null
                && spec.businessTargetTable() != null
                && !spec.businessTargetTable().isBlank()) {
            deleteRemovedRecordsInBatches(
                    businessTargetJdbc,
                    spec.businessTargetTable(),
                    spec.businessTargetPrimaryKey(),
                    orphans);
        }
        deleteRemovedRecordsInBatches(
                geoTargetJdbc, spec.geoTargetTable(), spec.geoTargetPrimaryKey(), orphans);

        mergeExtent(jobContext, orphanExtent, spec.layerName());
    }

    public Object normalizeId(Object id) {
        if (id == null) {
            return null;
        }
        if (id instanceof Number number) {
            return String.valueOf(number.longValue());
        }
        return id.toString().trim();
    }

    private Object[] bindNormalizedIds(Collection<Object> ids) {
        return ReconciliationPrimaryKeySql.bindNormalized(ids, this::normalizeId);
    }

    private void mergeExtent(ExecutionContext jobContext, String orphanExtent, String layerName) {
        if (orphanExtent == null || orphanExtent.isBlank()) {
            return;
        }
        synchronized (jobContext) {
            String existing = jobContext.containsKey(WatermarkContextKeys.AFFECTED_EXTENT)
                    ? jobContext.getString(WatermarkContextKeys.AFFECTED_EXTENT)
                    : null;
            jobContext.putString(
                    WatermarkContextKeys.AFFECTED_EXTENT,
                    AffectedExtent.union(existing, orphanExtent));
            if (layerName != null && !layerName.isBlank()) {
                jobContext.putString(WatermarkContextKeys.LAYER_NAME, layerName);
            }
        }
    }

    private void deleteOrphanChildrenInBatches(JdbcTemplate businessTargetJdbc,
                                               JdbcTemplate geoTargetJdbc,
                                               WatermarkTableSpec spec,
                                               Set<Object> orphans) {
        if (schemaIntrospectionService == null
                || !SyncKeys.AREA_OF_INTEREST.equals(spec.syncKey())) {
            return;
        }
        for (List<Object> batch : partition(orphans, reconciliationProperties.getDeleteBatchSize())) {
            deleteKpiMeasures(businessTargetJdbc, batch);
            deleteLayerFeatures(geoTargetJdbc, batch);
        }
    }

    private void deleteKpiMeasures(JdbcTemplate businessTargetJdbc, List<Object> orphanIds) {
        if (businessTargetJdbc == null || orphanIds.isEmpty()) {
            return;
        }
        QualifiedTable kpiTable = QualifiedTable.parse(KpiMeasureTableDdlBuilder.TABLE_NAME);
        if (!schemaIntrospectionService.tableExists(businessTargetJdbc, kpiTable)) {
            log.info("Table {} missing on the business database — KPI measures left untouched",
                    kpiTable.qualified());
            return;
        }
        int deleted = deleteByAreaOfInterestId(businessTargetJdbc, kpiTable, orphanIds);
        log.info("Area-of-interest orphans: removed {} KPI measure(s)", deleted);
    }

    private void deleteLayerFeatures(JdbcTemplate geoTargetJdbc, List<Object> orphanIds) {
        if (geoTargetJdbc == null || orphanIds.isEmpty()
                || layersProperties == null || layersProperties.getLayers() == null) {
            return;
        }
        for (LayerConfig layer : layersProperties.getLayers()) {
            QualifiedTable layerTable = layer.resolveTargetTable();
            if (!schemaIntrospectionService.tableExists(geoTargetJdbc, layerTable)) {
                log.info("Layer {} missing on the geo-target — features of the deleted area skipped",
                        layerTable.qualified());
                continue;
            }
            int deleted = deleteByAreaOfInterestId(geoTargetJdbc, layerTable, orphanIds);
            log.info("Area-of-interest orphans: removed {} feature(s) from {}",
                    deleted, layerTable.qualified());
        }
    }

    private int deleteByAreaOfInterestId(JdbcTemplate jdbc,
                                         QualifiedTable table,
                                         List<Object> orphanIds) {
        String placeholders = ReconciliationPrimaryKeySql.inPlaceholders(orphanIds.size());
        String sql = "DELETE FROM " + quote(table.schema()) + "." + quote(table.table())
                + " WHERE " + quote(LayerConfig.AREA_OF_INTEREST_ID_COLUMN)
                + " IN (" + placeholders + ")";
        return jdbc.update(sql, bindNormalizedIds(orphanIds));
    }

    private static String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private void deleteRemovedRecordsInBatches(JdbcTemplate targetJdbc,
                                               String table,
                                               String pk,
                                               Set<Object> idsToDelete) {
        for (List<Object> batch : partition(idsToDelete, reconciliationProperties.getDeleteBatchSize())) {
            deleteRemovedRecords(targetJdbc, table, pk, batch);
        }
    }

    private void deleteRemovedRecords(JdbcTemplate targetJdbc,
                                      String table,
                                      String pk,
                                      List<Object> idsToDelete) {
        if (idsToDelete.isEmpty()) {
            return;
        }
        String placeholders = ReconciliationPrimaryKeySql.inPlaceholders(idsToDelete.size());
        String sql = String.format("DELETE FROM %s WHERE %s IN (%s)", table, pk, placeholders);
        int deleted = targetJdbc.update(sql, bindNormalizedIds(idsToDelete));
        log.warn("Deleted {} inactive records from {}: {}", deleted, table, idsToDelete.size());
    }

    private void captureDepartedTerritoriesFromOrphans(JdbcTemplate geoTargetJdbc,
                                                       WatermarkTableSpec spec,
                                                       Set<Object> orphanIds,
                                                       ExecutionContext jobContext) {
        if (orphanIds.isEmpty()) {
            return;
        }
        String departedColumn = spec.geoTargetDepartedTerritoryColumn();
        Set<String> departedLevel3Ids = new HashSet<>();
        if (departedColumn != null && !departedColumn.isBlank()) {
            departedLevel3Ids.addAll(queryDepartedLevel3FromColumn(
                    geoTargetJdbc, spec, departedColumn, orphanIds));
        } else if (isGeographicLayerSpec(spec)) {
            departedLevel3Ids.addAll(queryDepartedLevel3FromLayerOrphans(
                    geoTargetJdbc, spec, orphanIds));
        } else {
            return;
        }

        if (!departedLevel3Ids.isEmpty()) {
            DepartedLevel3ContextSupport.merge(jobContext, departedLevel3Ids);
            log.info("Orphan reconciliation: {} departed level 3 territor(ies) captured before delete",
                    departedLevel3Ids.size());
        }
    }

    private static boolean isGeographicLayerSpec(WatermarkTableSpec spec) {
        String syncKey = spec.syncKey();
        return syncKey != null && syncKey.startsWith(LayerConfig.TARGET_SCHEMA + "_");
    }

    private Set<String> queryDepartedLevel3FromColumn(JdbcTemplate geoTargetJdbc,
                                                      WatermarkTableSpec spec,
                                                      String departedColumn,
                                                      Set<Object> orphanIds) {
        Set<String> departedLevel3Ids = new HashSet<>();
        for (List<Object> batch : partition(orphanIds, reconciliationProperties.getDeleteBatchSize())) {
            String placeholders = ReconciliationPrimaryKeySql.inPlaceholders(batch.size());
            String sql = String.format(
                    "SELECT DISTINCT %s FROM %s WHERE %s IN (%s) AND %s IS NOT NULL",
                    departedColumn,
                    spec.geoTargetTable(),
                    spec.geoTargetPrimaryKey(),
                    placeholders,
                    departedColumn);
            geoTargetJdbc.query(sql, rs -> {
                Object value = rs.getObject(1);
                if (value != null) {
                    departedLevel3Ids.add(value.toString().trim());
                }
            }, bindNormalizedIds(batch));
        }
        return departedLevel3Ids;
    }

    private Set<String> queryDepartedLevel3FromLayerOrphans(JdbcTemplate geoTargetJdbc,
                                                            WatermarkTableSpec spec,
                                                            Set<Object> orphanIds) {
        Set<String> departedLevel3Ids = new HashSet<>();
        for (List<Object> batch : partition(orphanIds, reconciliationProperties.getDeleteBatchSize())) {
            String placeholders = ReconciliationPrimaryKeySql.inPlaceholders(batch.size());
            String sql = String.format(
                    "SELECT DISTINCT aoi.%s FROM %s l"
                            + " INNER JOIN %s aoi ON aoi.%s = l.%s"
                            + " WHERE l.%s IN (%s) AND aoi.%s IS NOT NULL",
                    AreaOfInterestConfig.TERRITORY_LEVEL_3_ID_COLUMN,
                    spec.geoTargetTable(),
                    AreaOfInterestConfig.DEFAULT_TARGET_TABLE,
                    AreaOfInterestConfig.ID_COLUMN,
                    LayerConfig.AREA_OF_INTEREST_ID_COLUMN,
                    spec.geoTargetPrimaryKey(),
                    placeholders,
                    AreaOfInterestConfig.TERRITORY_LEVEL_3_ID_COLUMN);
            geoTargetJdbc.query(sql, rs -> {
                Object value = rs.getObject(1);
                if (value != null) {
                    departedLevel3Ids.add(value.toString().trim());
                }
            }, bindNormalizedIds(batch));
        }
        return departedLevel3Ids;
    }

    static List<List<Object>> partition(Collection<Object> ids, int batchSize) {
        List<Object> list = new ArrayList<>(ids);
        List<List<Object>> batches = new ArrayList<>();
        for (int i = 0; i < list.size(); i += batchSize) {
            batches.add(list.subList(i, Math.min(i + batchSize, list.size())));
        }
        return batches;
    }

}
