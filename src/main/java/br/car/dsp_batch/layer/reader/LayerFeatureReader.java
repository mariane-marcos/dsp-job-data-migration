package br.car.dsp_batch.layer.reader;

import br.car.dsp_batch.batch.reader.AbstractPartitionedPagingItemReader;
import br.car.dsp_batch.geometry.GeometrySql;
import br.car.dsp_batch.layer.dto.LayerFeatureRecord;
import br.car.dsp_batch.layer.metadata.ColumnMetadata;
import br.car.dsp_batch.layer.metadata.LayerTableMetadata;
import br.car.dsp_batch.sync.WatermarkSql;
import br.car.dsp_batch.temporal.CommonTemporalHandler;
import br.car.dsp_batch.temporal.TemporalTypeClassifier;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.PostgresPagingQueryProvider;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Generic paging reader for layer features.
 * When a watermark is present, only features updated after it are read.
 */
public class LayerFeatureReader extends AbstractPartitionedPagingItemReader<LayerFeatureRecord> {

    public LayerFeatureReader(DataSource dataSource,
                              Long minId,
                              Long maxId,
                              int pageSize,
                              LayerTableMetadata metadata,
                              Instant watermark,
                              String readerName) {
        super(dataSource, minId, maxId, pageSize);
        this.setName(readerName);
        boolean useIdRange = minId != null && maxId != null;
        this.setQueryProvider(createQueryProvider(metadata, useIdRange, watermark));
        this.setRowMapper(new LayerFeatureRowMapper(metadata));
    }

    private PagingQueryProvider createQueryProvider(LayerTableMetadata metadata,
                                                    boolean useIdRange,
                                                    Instant watermark) {
        String partitionColumn = metadata.primaryKeyColumn();
        String geom = metadata.geometryColumn();
        List<String> persistColumns = new ArrayList<>(metadata.sourceNonGeometryColumnNames());

        String selectColumns = String.join(", ", persistColumns);
        if (!persistColumns.contains(partitionColumn)) {
            selectColumns = selectColumns + ", " + partitionColumn;
        }

        int srid = metadata.srid();
        String transformedGeom = GeometrySql.transform(geom, srid);

        PostgresPagingQueryProvider queryProvider = new PostgresPagingQueryProvider();
        queryProvider.setSelectClause(
                "SELECT " + selectColumns
                        + ", " + GeometrySql.asEwkb2d(transformedGeom)
                        + " AS geometry_ewkb"
        );
        queryProvider.setFromClause("FROM " + metadata.qualifiedSourceTable());

        String geometryFilter = GeometrySql.validNonEmptyPredicate(geom);

        String configWhere = metadata.whereClause();
        boolean hasConfigWhere = configWhere != null
                && !configWhere.isBlank()
                && !"1=1".equals(configWhere.trim());

        StringBuilder where = new StringBuilder("WHERE ").append(geometryFilter);
        if (hasConfigWhere) {
            where.append(" AND (").append(configWhere).append(")");
        }

        String changeFilter = WatermarkSql.buildChangeDetectionFilter(
                metadata.creationDateColumn(),
                metadata.updatedAtColumn(),
                watermark);
        if (changeFilter != null) {
            where.append(" AND ").append(changeFilter);
        }

        if (useIdRange) {
            String numericPartitionColumn = "CAST(" + partitionColumn + " AS BIGINT)";
            where.append(" AND ").append(numericPartitionColumn).append(" >= :minId")
                    .append(" AND ").append(numericPartitionColumn).append(" <= :maxId");
        }

        queryProvider.setWhereClause(where.toString());

        Map<String, Order> sortKeys = new HashMap<>();
        sortKeys.put(partitionColumn, Order.ASCENDING);
        queryProvider.setSortKeys(sortKeys);

        return queryProvider;
    }

    private static class LayerFeatureRowMapper implements RowMapper<LayerFeatureRecord> {

        private final LayerTableMetadata metadata;
        private final Map<String, String> udtByColumn;

        LayerFeatureRowMapper(LayerTableMetadata metadata) {
            this.metadata = metadata;
            this.udtByColumn = new HashMap<>();
            for (ColumnMetadata column : metadata.columns()) {
                udtByColumn.put(column.name(), column.udtName());
            }
        }

        @Override
        public LayerFeatureRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
            LayerFeatureRecord record = new LayerFeatureRecord();
            record.setId(rs.getObject(metadata.primaryKeyColumn()));
            record.setGeometryEwkb(rs.getBytes("geometry_ewkb"));

            for (String column : metadata.sourceNonGeometryColumnNames()) {
                String udt = udtByColumn.get(column);
                if (udt != null && TemporalTypeClassifier.isTemporal(udt)) {
                    record.putAttribute(column, CommonTemporalHandler.read(rs, column, udt));
                } else {
                    record.putAttribute(column, rs.getObject(column));
                }
            }

            return record;
        }
    }
}
