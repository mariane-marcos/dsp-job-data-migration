package br.car.dsp_batch.reconciliation;

import br.car.dsp_batch.geometry.GeometrySql;
import br.car.dsp_batch.sync.AffectedExtent;
import br.car.dsp_batch.sync.WatermarkTableSpec;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Keyset pages of geo-target primary keys, plus one extent for ids that will be deleted.
 * Destination primary keys are varchar, so the page compares the column as stored.
 */
public class GeoTargetOrphanBatchReader {

    private final JdbcTemplate geoTargetJdbc;
    private final WatermarkTableSpec spec;
    private final String rangeEndInclusive;

    public GeoTargetOrphanBatchReader(JdbcTemplate geoTargetJdbc,
                                      WatermarkTableSpec spec,
                                      String rangeEndInclusive) {
        this.geoTargetJdbc = geoTargetJdbc;
        this.spec = spec;
        this.rangeEndInclusive = rangeEndInclusive;
    }

    public List<String> fetchNextBatch(String lastExclusiveKey, int limit) {
        String pk = spec.geoTargetPrimaryKey();
        String table = spec.geoTargetTable();
        StringBuilder sql = new StringBuilder("SELECT ").append(pk)
                .append(" FROM ").append(table)
                .append(" WHERE 1=1");
        List<Object> params = new ArrayList<>();
        if (lastExclusiveKey != null) {
            sql.append(" AND ").append(pk).append(" > ?");
            params.add(lastExclusiveKey);
        }
        if (rangeEndInclusive != null) {
            sql.append(" AND ").append(pk).append(" <= ?");
            params.add(rangeEndInclusive);
        }
        sql.append(" ORDER BY ").append(pk).append(" LIMIT ?");
        params.add(limit);

        List<String> ids = new ArrayList<>();
        geoTargetJdbc.query(sql.toString(), rs -> {
            String id = rs.getString(1);
            if (id == null) {
                throw new IllegalStateException("Null PK on " + table);
            }
            ids.add(id);
        }, params.toArray());
        return ids;
    }

    /**
     * @return one EPSG:3857 extent covering the given ids, or null when none have a geometry
     */
    public String fetchExtent(Collection<String> rawIds) {
        if (rawIds == null || rawIds.isEmpty()) {
            return null;
        }
        String sql = extentSql(spec);
        Object[] values = rawIds.toArray();
        return geoTargetJdbc.execute((ConnectionCallback<String>) connection -> {
            Array array = connection.createArrayOf("varchar", values);
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                ReconciliationPrimaryKeySql.disableServerPrepare(statement);
                statement.setArray(1, array);
                try (ResultSet rs = statement.executeQuery()) {
                    if (!rs.next()) {
                        return null;
                    }
                    double minX = rs.getDouble(1);
                    if (rs.wasNull()) {
                        return null;
                    }
                    return AffectedExtent.format(minX, rs.getDouble(2), rs.getDouble(3), rs.getDouble(4));
                }
            } finally {
                array.free();
            }
        });
    }

    static String extentSql(WatermarkTableSpec spec) {
        String pk = spec.geoTargetPrimaryKey();
        String geom3857 = GeometrySql.inWebMercator(spec.geoTargetGeometryColumn(), spec.srid());
        return "SELECT ST_XMin(ext), ST_YMin(ext), ST_XMax(ext), ST_YMax(ext) "
                + "FROM (SELECT ST_Extent(" + geom3857 + ") AS ext "
                + "FROM " + spec.geoTargetTable()
                + " WHERE " + pk + " = ANY(?)) agg";
    }
}
