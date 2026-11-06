package br.car.dsp_batch.reconciliation.partitioner;

import br.car.dsp_batch.sync.WatermarkTableSpec;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Splits a geo-target orphan scan into PK ranges. Destination PKs are varchar,
 * so each worker walks {@code id > start AND id <= end} on the primary-key index.
 */
public class GeoTargetReconciliationPartitioner implements Partitioner {

    public static final String RANGE_START = "reconciliationRangeStart";
    public static final String RANGE_END = "reconciliationRangeEnd";

    private final JdbcTemplate geoTargetJdbc;
    private final WatermarkTableSpec spec;

    public GeoTargetReconciliationPartitioner(JdbcTemplate geoTargetJdbc, WatermarkTableSpec spec) {
        this.geoTargetJdbc = geoTargetJdbc;
        this.spec = spec;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        int size = Math.max(1, gridSize);
        if (size == 1) {
            return singlePartition();
        }
        long count = countRows();
        if (count == 0) {
            return singlePartition();
        }

        List<String> cuts = new ArrayList<>();
        for (int i = 1; i < size; i++) {
            cuts.add(pkAtOffset(count * i / size));
        }

        Map<String, ExecutionContext> result = new HashMap<>();
        for (int i = 0; i < size; i++) {
            ExecutionContext ctx = new ExecutionContext();
            if (i > 0) {
                ctx.putString(RANGE_START, cuts.get(i - 1));
            }
            if (i < size - 1) {
                ctx.putString(RANGE_END, cuts.get(i));
            }
            result.put("partition" + i, ctx);
        }
        return result;
    }

    private Map<String, ExecutionContext> singlePartition() {
        Map<String, ExecutionContext> result = new HashMap<>();
        result.put("partition0", new ExecutionContext());
        return result;
    }

    private long countRows() {
        Long count = geoTargetJdbc.queryForObject(
                "SELECT COUNT(*) FROM " + spec.geoTargetTable(), Long.class);
        return count == null ? 0L : count;
    }

    private String pkAtOffset(long offset) {
        String pk = spec.geoTargetPrimaryKey();
        String table = spec.geoTargetTable();
        // shortcut: OFFSET percorre o índice uma vez por corte, trocar por uma passagem com row_number() se o setup aparecer no tempo.
        String sql = "SELECT " + pk + " FROM " + table + " ORDER BY " + pk + " LIMIT 1 OFFSET ?";
        List<String> found = geoTargetJdbc.query(sql, (rs, rowNum) -> rs.getString(1), offset);
        if (found.isEmpty() || found.getFirst() == null || found.getFirst().isBlank()) {
            throw new IllegalStateException("Missing geo-target PK at offset " + offset + " on " + table);
        }
        return found.getFirst();
    }
}
