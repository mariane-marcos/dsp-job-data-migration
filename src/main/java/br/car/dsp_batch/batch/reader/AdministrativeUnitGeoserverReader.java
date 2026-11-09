package br.car.dsp_batch.batch.reader;

import br.car.dsp_batch.batch.config.JobTableConfig;
import br.car.dsp_batch.batch.dto.AdministrativeUnitDTO;
import br.car.dsp_batch.geometry.GeometrySql;
import br.car.dsp_batch.sync.WatermarkSql;
import br.car.dsp_batch.temporal.CommonTemporalHandler;
import br.car.dsp_batch.temporal.TemporalColumnSpecs;
import br.car.dsp_batch.temporal.TemporalTypeClassifier;
import br.car.dsp_batch.temporal.TemporalColumnSpecs;
import br.car.dsp_batch.temporal.WatermarkTemporalBridge;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.PostgresPagingQueryProvider;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Generic paging reader driven by {@link JobTableConfig}.
 */
public class AdministrativeUnitGeoserverReader
        extends AbstractPartitionedPagingItemReader<AdministrativeUnitDTO> {

    public AdministrativeUnitGeoserverReader(DataSource dataSource,
                                             Long minId,
                                             Long maxId,
                                             int pageSize,
                                             JobTableConfig tableConfig,
                                             String readerName) {
        this(dataSource, minId, maxId, pageSize, tableConfig, null, null, readerName);
    }

    public AdministrativeUnitGeoserverReader(DataSource dataSource,
                                             Long minId,
                                             Long maxId,
                                             int pageSize,
                                             JobTableConfig tableConfig,
                                             Instant watermark,
                                             TemporalColumnSpecs temporalColumns,
                                             String readerName) {
        super(dataSource, minId, maxId, pageSize);
        this.setName(readerName);
        boolean useIdRange = minId != null && maxId != null;
        this.setQueryProvider(createQueryProvider(
                tableConfig, useIdRange, watermark, temporalColumns));
        this.setRowMapper(new AdministrativeUnitRowMapper(tableConfig));
    }

    private PagingQueryProvider createQueryProvider(JobTableConfig tableConfig,
                                                    boolean useIdRange,
                                                    Instant watermark,
                                                    TemporalColumnSpecs temporalColumns) {
        String partitionColumn = tableConfig.getPartitionColumn();
        String primaryKey = tableConfig.getPrimaryKey();
        String geom = tableConfig.getGeometryColumn();
        List<String> persistColumns = new ArrayList<>(tableConfig.getPersistColumns());

        String selectColumns = String.join(", ", persistColumns);

        // The partition column may repeat the same text. The primary key
        // breaks the tie; otherwise the next page drops rows equal to the last one read.
        boolean tieBreakWithPrimaryKey = primaryKey != null
                && !primaryKey.isBlank()
                && !primaryKey.equals(partitionColumn);

        if (partitionColumn != null && !partitionColumn.isBlank() && !persistColumns.contains(partitionColumn)) {
            selectColumns = selectColumns + ", " + partitionColumn;
        }
        if (tieBreakWithPrimaryKey && !persistColumns.contains(primaryKey)) {
            selectColumns = selectColumns + ", " + primaryKey;
        }

        int srid = tableConfig.getSrid();
        String transformedGeom = GeometrySql.transform(geom, srid);

        PostgresPagingQueryProvider queryProvider = new PostgresPagingQueryProvider();
        queryProvider.setSelectClause(
                "SELECT " + selectColumns
                        + ", " + GeometrySql.asEwkb2d(transformedGeom)
                        + " AS geometry_ewkb"
        );
        queryProvider.setFromClause("FROM " + tableConfig.getSourceTable());

        String geometryFilter = GeometrySql.validNonEmptyPredicate(geom);

        // YAML where-clause (sample / adopter filters) must also apply on the write read.
        String configWhere = tableConfig.getWhereClause();
        boolean hasConfigWhere = configWhere != null
                && !configWhere.isBlank()
                && !"1=1".equals(configWhere.trim());

        StringBuilder where = new StringBuilder("WHERE ").append(geometryFilter);
        if (hasConfigWhere) {
            where.append(" AND (").append(configWhere).append(")");
        }

        if (temporalColumns != null) {
            String changeFilter = WatermarkSql.buildChangeDetectionFilter(
                    temporalColumns.creationDateColumn(),
                    temporalColumns.updatedAtColumn(),
                    watermark);
            where.append(" AND ").append(changeFilter);
        }

        if (useIdRange) {
            // CAST handles VARCHAR columns containing numeric values (e.g., cd_uf);
            // minId/maxId are Long values.
            String numericPartitionColumn = "CAST(" + partitionColumn + " AS BIGINT)";
            where.append(" AND ").append(numericPartitionColumn).append(" >= :minId")
                    .append(" AND ").append(numericPartitionColumn).append(" <= :maxId");
        }

        queryProvider.setWhereClause(where.toString());

        Map<String, Order> sortKeys = new LinkedHashMap<>();
        sortKeys.put(partitionColumn, Order.ASCENDING);
        if (tieBreakWithPrimaryKey) {
            sortKeys.put(primaryKey, Order.ASCENDING);
        }
        queryProvider.setSortKeys(sortKeys);

        return queryProvider;
    }

    private static class AdministrativeUnitRowMapper implements RowMapper<AdministrativeUnitDTO> {

        private final JobTableConfig tableConfig;

        AdministrativeUnitRowMapper(JobTableConfig tableConfig) {
            this.tableConfig = tableConfig;
        }

        @Override
        public AdministrativeUnitDTO mapRow(ResultSet rs, int rowNum) throws SQLException {
            AdministrativeUnitDTO dto = new AdministrativeUnitDTO();
            dto.setId(rs.getObject(tableConfig.getPrimaryKey()));
            dto.setGeometryEwkb(rs.getBytes("geometry_ewkb"));

            ResultSetMetaData meta = rs.getMetaData();
            Map<String, String> udtByLabel = new HashMap<>();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                String label = meta.getColumnLabel(i);
                if (label != null) {
                    udtByLabel.put(label.toLowerCase(Locale.ROOT), meta.getColumnTypeName(i));
                }
            }

            for (String column : tableConfig.getPersistColumns()) {
                String udt = udtByLabel.get(column.toLowerCase(Locale.ROOT));
                if (udt != null && TemporalTypeClassifier.isTemporal(udt)) {
                    dto.putAttribute(column, CommonTemporalHandler.read(rs, column, udt));
                } else {
                    dto.putAttribute(column, rs.getObject(column));
                }
            }

            return dto;
        }
    }
}
