package br.car.dsp_batch.reconciliation;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;

/**
 * SQL helpers for orphan deletes. Geo-target primary keys are varchar, so binds stay text.
 * The origin probe compares the source column with a typed array instead.
 */
public final class ReconciliationPrimaryKeySql {

    private ReconciliationPrimaryKeySql() {
    }

    public static String inPlaceholders(int count) {
        return String.join(",", Collections.nCopies(count, "?"));
    }

    public static Object[] bindNormalized(Collection<?> ids, Function<Object, Object> normalizeId) {
        return ids.stream().map(normalizeId).toArray();
    }

    /**
     * O driver JDBC está em escopo runtime. Plano genérico de {@code = ANY($1)} pode ignorar o índice.
     */
    static void disableServerPrepare(PreparedStatement statement) {
        try {
            Class<?> pgStatement = Class.forName("org.postgresql.PGStatement");
            if (!statement.isWrapperFor(pgStatement)) {
                return;
            }
            Object pg = statement.unwrap(pgStatement);
            pg.getClass().getMethod("setPrepareThreshold", int.class).invoke(pg, 0);
        } catch (ReflectiveOperationException | SQLException ex) {
            throw new IllegalStateException("Could not disable server-side prepare for orphan probe", ex);
        }
    }
}
