package br.car.dsp_batch.reconciliation;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ReconciliationTableSpecResolverTest {

    @Test
    void requireSourcePkUdt_ReadsInformationSchema() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.query(anyString(), any(RowMapper.class), eq("src"), eq("l1"), eq("id")))
                .thenReturn(List.of("int4"));

        assertEquals("int4", ReconciliationTableSpecResolver.requireSourcePkUdt(jdbc, "src.l1", "id"));
    }

    @Test
    void requireSourcePkUdt_MissingColumnFails() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.query(anyString(), any(RowMapper.class), eq("src"), eq("l1"), eq("id")))
                .thenReturn(List.of());

        IllegalStateException ex = assertThrows(
                IllegalStateException.class,
                () -> ReconciliationTableSpecResolver.requireSourcePkUdt(jdbc, "src.l1", "id"));
        assertTrue(ex.getMessage().contains("src.l1"));
    }
}
