package br.car.dsp_batch.reconciliation;

import br.car.dsp_batch.geometry.GeometrySql;
import br.car.dsp_batch.sync.WatermarkTableSpec;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

/**
 * Read-only probe on the source for PKs that still exist and match eligibility rules.
 * The PK column is compared as itself so the primary-key index can be used.
 */
@Component
public class OrphanOriginProbe {

    private final OrphanReconciliationService reconciliationService;

    public OrphanOriginProbe(OrphanReconciliationService reconciliationService) {
        this.reconciliationService = reconciliationService;
    }

    public Set<Object> probeExistingNormalizedIds(JdbcTemplate sourceJdbc,
                                                  WatermarkTableSpec spec,
                                                  Collection<?> candidateIds) {
        if (candidateIds == null || candidateIds.isEmpty()) {
            return Set.of();
        }
        String udt = spec.sourcePrimaryKeyUdtName();
        if (udt == null || udt.isBlank()) {
            throw new IllegalStateException(
                    "Source PK type is required to probe orphans on " + spec.sourceTable());
        }

        List<Object> values = new ArrayList<>();
        for (Object raw : candidateIds) {
            Object normalized = reconciliationService.normalizeId(raw);
            if (normalized == null) {
                continue;
            }
            Object converted = toSourcePkValue(udt, normalized.toString());
            if (converted != null) {
                values.add(converted);
            }
        }
        if (values.isEmpty()) {
            return Set.of();
        }

        String sql = probeSql(spec);
        return sourceJdbc.execute((ConnectionCallback<Set<Object>>) connection -> {
            Array array = connection.createArrayOf(udt, values.toArray());
            Set<Object> ids = new HashSet<>();
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                ReconciliationPrimaryKeySql.disableServerPrepare(statement);
                statement.setArray(1, array);
                try (ResultSet rs = statement.executeQuery()) {
                    while (rs.next()) {
                        ids.add(reconciliationService.normalizeId(rs.getObject(1)));
                    }
                }
            } finally {
                array.free();
            }
            return ids;
        });
    }

    static String probeSql(WatermarkTableSpec spec) {
        String pk = spec.sourcePrimaryKey();
        String geom = spec.sourceGeometryColumn();
        String table = spec.sourceTable();
        String validGeomFilter = GeometrySql.validNonEmptyPredicate(geom);
        String configWhere = spec.whereClause();
        boolean hasConfigWhere = configWhere != null
                && !configWhere.isBlank()
                && !"1=1".equals(configWhere.trim());

        StringBuilder sql = new StringBuilder("SELECT ").append(pk)
                .append(" FROM ").append(table)
                .append(" WHERE ").append(pk).append(" = ANY(?)")
                .append(" AND ").append(validGeomFilter);
        if (hasConfigWhere) {
            sql.append(" AND (").append(configWhere).append(")");
        }
        return sql.toString();
    }

    /**
     * @return the bind value for the source column, or null when the text cannot be that type
     * (the id is then treated as absent on the source)
     */
    static Object toSourcePkValue(String udtName, String normalized) {
        String udt = udtName.toLowerCase(Locale.ROOT).trim();
        try {
            return switch (udt) {
                case "int2" -> canonicalShort(normalized);
                case "int4" -> canonicalInt(normalized);
                case "int8" -> canonicalLong(normalized);
                case "varchar", "text", "bpchar" -> normalized;
                case "uuid" -> parseUuid(normalized);
                default -> throw new IllegalStateException(
                        "Unsupported source PK type '" + udtName + "' for orphan probe");
            };
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private static Short canonicalShort(String text) {
        short value = Short.parseShort(text);
        if (!Short.toString(value).equals(text)) {
            return null;
        }
        return value;
    }

    private static Integer canonicalInt(String text) {
        int value = Integer.parseInt(text);
        if (!Integer.toString(value).equals(text)) {
            return null;
        }
        return value;
    }

    private static UUID parseUuid(String text) {
        try {
            return UUID.fromString(text);
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }

    private static Long canonicalLong(String text) {
        long value = Long.parseLong(text);
        if (!Long.toString(value).equals(text)) {
            return null;
        }
        return value;
    }
}
