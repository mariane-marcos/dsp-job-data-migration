package br.car.dsp_batch.reconciliation;

import br.car.dsp_batch.sync.SyncKeys;
import br.car.dsp_batch.sync.WatermarkTableSpec;
import br.car.dsp_batch.temporal.TemporalTestFixtures;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.postgresql.PGStatement;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrphanOriginProbeTest {

    @Test
    void probeExistingNormalizedIds_EmptyCandidatesSkipsQuery() throws Exception {
        OrphanOriginProbe probe = probe();
        JdbcTemplate sourceJdbc = mock(JdbcTemplate.class);

        Set<Object> found = probe.probeExistingNormalizedIds(sourceJdbc, sampleSpec("int4"), List.of());

        assertTrue(found.isEmpty());
        verify(sourceJdbc, never()).execute(any(ConnectionCallback.class));
    }

    @Test
    void probeExistingNormalizedIds_UnparseableIntegerSkipsQuery() throws Exception {
        OrphanOriginProbe probe = probe();
        JdbcTemplate sourceJdbc = mock(JdbcTemplate.class);

        Set<Object> found = probe.probeExistingNormalizedIds(sourceJdbc, sampleSpec("int4"), List.of("abc"));

        assertTrue(found.isEmpty());
        verify(sourceJdbc, never()).execute(any(ConnectionCallback.class));
    }

    @Test
    void probeExistingNormalizedIds_UsesTypedArrayAndKeepsGeometryFilter() throws Exception {
        OrphanOriginProbe probe = probe();
        JdbcTemplate sourceJdbc = mock(JdbcTemplate.class);
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        Array sqlArray = mock(Array.class);
        PGStatement pgStatement = mock(PGStatement.class);

        when(connection.createArrayOf(eq("int4"), any(Object[].class))).thenReturn(sqlArray);
        when(connection.prepareStatement(any(String.class))).thenReturn(statement);
        when(statement.isWrapperFor(PGStatement.class)).thenReturn(true);
        when(statement.unwrap(PGStatement.class)).thenReturn(pgStatement);
        when(statement.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getObject(1)).thenReturn(10);
        when(sourceJdbc.execute(any(ConnectionCallback.class))).thenAnswer(invocation -> {
            ConnectionCallback<?> callback = invocation.getArgument(0);
            return callback.doInConnection(connection);
        });

        Set<Object> found = probe.probeExistingNormalizedIds(
                sourceJdbc, sampleSpec("int4", "active = true"), List.of(10L, "abc", "20"));

        assertEquals(Set.of("10"), found);
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(connection).prepareStatement(sqlCaptor.capture());
        String sql = sqlCaptor.getValue();
        assertTrue(sql.contains("id = ANY(?)"));
        assertTrue(sql.contains("ST_MakeValid(geom)"));
        assertTrue(sql.contains("AND (active = true)"));
        assertTrue(sql.contains("FROM src.l1"));
        assertFalse(sql.contains("TRIM(CAST"));

        ArgumentCaptor<Object[]> values = ArgumentCaptor.forClass(Object[].class);
        verify(connection).createArrayOf(eq("int4"), values.capture());
        assertEquals(2, values.getValue().length);
        assertEquals(10, values.getValue()[0]);
        assertEquals(20, values.getValue()[1]);
        verify(pgStatement).setPrepareThreshold(0);
        verify(sqlArray).free();
    }

    @Test
    void probeExistingNormalizedIds_BindsVarcharTextFromNumericDestinationId() throws Exception {
        OrphanOriginProbe probe = probe();
        JdbcTemplate sourceJdbc = mock(JdbcTemplate.class);
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        Array sqlArray = mock(Array.class);

        when(connection.createArrayOf(eq("varchar"), any(Object[].class))).thenReturn(sqlArray);
        when(connection.prepareStatement(any(String.class))).thenReturn(statement);
        when(statement.isWrapperFor(PGStatement.class)).thenReturn(false);
        when(statement.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(false);
        when(sourceJdbc.execute(any(ConnectionCallback.class))).thenAnswer(invocation -> {
            ConnectionCallback<?> callback = invocation.getArgument(0);
            return callback.doInConnection(connection);
        });

        probe.probeExistingNormalizedIds(sourceJdbc, sampleSpec("varchar"), List.of(10L, " 20 "));

        ArgumentCaptor<Object[]> values = ArgumentCaptor.forClass(Object[].class);
        verify(connection).createArrayOf(eq("varchar"), values.capture());
        assertEquals("10", values.getValue()[0]);
        assertEquals("20", values.getValue()[1]);
    }

    @Test
    void probeExistingNormalizedIds_BindsUuidAndSkipsInvalidText() throws Exception {
        OrphanOriginProbe probe = probe();
        JdbcTemplate sourceJdbc = mock(JdbcTemplate.class);
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        Array sqlArray = mock(Array.class);
        UUID id = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");

        when(connection.createArrayOf(eq("uuid"), any(Object[].class))).thenReturn(sqlArray);
        when(connection.prepareStatement(any(String.class))).thenReturn(statement);
        when(statement.isWrapperFor(any(Class.class))).thenReturn(false);
        when(statement.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getObject(1)).thenReturn(id);
        when(sourceJdbc.execute(any(ConnectionCallback.class))).thenAnswer(invocation -> {
            ConnectionCallback<?> callback = invocation.getArgument(0);
            return callback.doInConnection(connection);
        });

        Set<Object> found = probe.probeExistingNormalizedIds(
                sourceJdbc, sampleSpec("uuid"), List.of(id.toString(), "abc"));

        assertEquals(Set.of(id.toString()), found);
        ArgumentCaptor<Object[]> values = ArgumentCaptor.forClass(Object[].class);
        verify(connection).createArrayOf(eq("uuid"), values.capture());
        assertEquals(1, values.getValue().length);
        assertEquals(id, values.getValue()[0]);
    }

    @Test
    void toSourcePkValue_ConvertsCanonicalNumbersAndText() {
        assertEquals(10, OrphanOriginProbe.toSourcePkValue("int4", "10"));
        assertEquals(10L, OrphanOriginProbe.toSourcePkValue("int8", "10"));
        assertEquals((short) 10, OrphanOriginProbe.toSourcePkValue("int2", "10"));
        assertEquals("10", OrphanOriginProbe.toSourcePkValue("text", "10"));
        assertEquals("10", OrphanOriginProbe.toSourcePkValue("bpchar", "10"));
        assertNull(OrphanOriginProbe.toSourcePkValue("int4", "abc"));
        assertNull(OrphanOriginProbe.toSourcePkValue("int8", "010"));
        assertNull(OrphanOriginProbe.toSourcePkValue("int4", "10.0"));
        assertEquals(
                UUID.fromString("550e8400-e29b-41d4-a716-446655440000"),
                OrphanOriginProbe.toSourcePkValue("uuid", "550e8400-e29b-41d4-a716-446655440000"));
        assertNull(OrphanOriginProbe.toSourcePkValue("uuid", "abc"));
    }

    @Test
    void toSourcePkValue_RejectsUnsupportedType() {
        IllegalStateException ex = assertThrows(
                IllegalStateException.class,
                () -> OrphanOriginProbe.toSourcePkValue("numeric", "10"));
        assertTrue(ex.getMessage().contains("numeric"));
    }

    @Test
    void probeExistingNormalizedIds_RequiresSourcePkType() {
        OrphanOriginProbe probe = probe();
        IllegalStateException ex = assertThrows(
                IllegalStateException.class,
                () -> probe.probeExistingNormalizedIds(mock(JdbcTemplate.class), sampleSpec(null), List.of(1)));
        assertTrue(ex.getMessage().contains("src.l1"));
    }

    private static OrphanOriginProbe probe() {
        return new OrphanOriginProbe(new OrphanReconciliationService(new ReconciliationProperties()));
    }

    private static WatermarkTableSpec sampleSpec(String udt) {
        return sampleSpec(udt, "1=1");
    }

    private static WatermarkTableSpec sampleSpec(String udt, String where) {
        return new WatermarkTableSpec(
                SyncKeys.ADMIN_UNIT_LEVEL_1,
                "src.l1",
                "id",
                "geom",
                TemporalTestFixtures.timestamptz("created_at"),
                TemporalTestFixtures.timestamptz("updated_at"),
                where,
                4326,
                "layer",
                "dsp.l1",
                "id",
                "geom",
                null,
                null,
                null,
                udt
        );
    }
}
