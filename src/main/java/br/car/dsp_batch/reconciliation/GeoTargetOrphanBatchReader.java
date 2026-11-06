package br.car.dsp_batch.reconciliation;

import br.car.dsp_batch.geometry.GeometrySql;
import br.car.dsp_batch.sync.WatermarkTableSpec;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Keyset pages of geo-target primary keys, plus bbox lookup for ids that will be deleted.
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

    public Map<String, String> fetchBboxes(Collection<String> rawIds) {
        if (rawIds == null || rawIds.isEmpty()) {
            return Map.of();
        }
        String sql = orphanBboxSql(spec);
        Object[] values = rawIds.toArray();
        return geoTargetJdbc.execute((ConnectionCallback<Map<String, String>>) connection -> {
            Array array = connection.createArrayOf("varchar", values);
            Map<String, String> bboxes = new HashMap<>();
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                ReconciliationPrimaryKeySql.disableServerPrepare(statement);
                statement.setArray(1, array);
                try (ResultSet rs = statement.executeQuery()) {
                    while (rs.next()) {
                        String id = rs.getString(1);
                        double minX = rs.getDouble(2);
                        if (rs.wasNull()) {
                            continue;
                        }
                        double minY = rs.getDouble(3);
                        double maxX = rs.getDouble(4);
                        double maxY = rs.getDouble(5);
                        bboxes.put(id, OrphanReconciliationService.formatBbox(minX, minY, maxX, maxY));
                    }
                }
            } finally {
                array.free();
            }
            return bboxes;
        });
    }

    static String orphanBboxSql(WatermarkTableSpec spec) {
        String pk = spec.geoTargetPrimaryKey();
        String geom = spec.geoTargetGeometryColumn();
        int srid = spec.srid();
        String geom2d = GeometrySql.force2d(geom);
        return "SELECT " + pk + ", "
                + "ST_XMin(env3857), ST_YMin(env3857), ST_XMax(env3857), ST_YMax(env3857) "
                + "FROM (SELECT " + pk + ", "
                + "ST_Transform(ST_Envelope(ST_SetSRID(" + geom2d + ", " + srid + ")), 3857) AS env3857 "
                + "FROM " + spec.geoTargetTable()
                + " WHERE " + pk + " = ANY(?)) bbox_q";
    }
}
