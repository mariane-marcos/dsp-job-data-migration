package br.car.dsp_batch.sync;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AffectedExtentTest {

    @Test
    void union_KeepsTheBoxThatContainsBoth() {
        assertEquals(
                "0.000000,0.000000,3.000000,4.000000",
                AffectedExtent.union(
                        "0.000000,1.000000,2.000000,3.000000",
                        "1.000000,0.000000,3.000000,4.000000"));
    }

    @Test
    void union_IgnoresBlankSide() {
        assertEquals("1,2,3,4", AffectedExtent.union(null, "1,2,3,4"));
        assertEquals("1,2,3,4", AffectedExtent.union("1,2,3,4", " "));
    }
}
