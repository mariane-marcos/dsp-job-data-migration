package br.car.dsp_batch.geometry;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GeometrySqlTest {

    @Test
    void validNonEmptyPredicate_AcceptsAnyGeometryType() {
        String predicate = GeometrySql.validNonEmptyPredicate("geom");

        assertEquals("(geom IS NOT NULL AND NOT ST_IsEmpty(ST_MakeValid(geom)))", predicate);
        assertFalse(predicate.contains("CollectionExtract"));
        assertFalse(predicate.contains(", 3)"));
    }

    @Test
    void validNonEmptyPredicate_UsesProvidedColumnName() {
        String predicate = GeometrySql.validNonEmptyPredicate("geometry");

        assertTrue(predicate.contains("geometry IS NOT NULL"));
        assertTrue(predicate.contains("ST_MakeValid(geometry)"));
    }

    @Test
    void validNonEmptyPredicate_RejectsBlankColumn() {
        assertThrows(IllegalArgumentException.class, () -> GeometrySql.validNonEmptyPredicate(" "));
        assertThrows(IllegalArgumentException.class, () -> GeometrySql.validNonEmptyPredicate(null));
    }

    @Test
    void force2d_WrapsExpression() {
        assertEquals("ST_Force2D(geom)", GeometrySql.force2d("geom"));
    }

    @Test
    void transform_WrapsExpressionWithTargetSrid() {
        assertEquals(
                "public.ST_Transform(the_geom, 4674)",
                GeometrySql.transform("the_geom", 4674)
        );
    }

    @Test
    void transform_RejectsInvalidSrid() {
        assertThrows(IllegalArgumentException.class, () -> GeometrySql.transform("geom", 0));
    }

    @Test
    void areaGeographySquareMetres_TransformsToWgs84BeforeGeographyCast() {
        assertEquals(
                "public.ST_Area(public.ST_Transform(ST_Force2D(\"geom\"), 4326)::geography)",
                GeometrySql.areaGeographySquareMetres("\"geom\"")
        );
    }

    @Test
    void asEwkb2d_Forces2dBeforeEwkb() {
        String sql = GeometrySql.asEwkb2d("geom");

        assertEquals("public.ST_AsEWKB(ST_Force2D(geom))", sql);
        assertTrue(sql.contains("ST_Force2D"));
    }

    @Test
    void inWebMercator_SetsSourceSridThenTransforms() {
        assertEquals(
                "public.ST_Transform(ST_SetSRID(ST_Force2D(geom), 4674), 3857)",
                GeometrySql.inWebMercator("geom", 4674)
        );
    }

    @Test
    void inWebMercator_RejectsInvalidSrid() {
        assertThrows(IllegalArgumentException.class, () -> GeometrySql.inWebMercator("geom", 0));
    }

    @Test
    void multiGeomFromEwkbParam_WrapsSingleBind() {
        assertEquals(
                "public.ST_Multi(public.ST_GeomFromEWKB(?))",
                GeometrySql.multiGeomFromEwkbParam()
        );
    }

    @Test
    void envelopeAndCentroidFromEwkb_UsesOneBind() {
        String sql = GeometrySql.envelopeAndCentroidFromEwkb();

        assertEquals(
                "public.ST_Envelope(g), public.ST_Centroid(g) FROM (SELECT public.ST_GeomFromEWKB(?) AS g) ewkb",
                sql
        );
        assertFalse(sql.contains("ST_GeomFromGeoJSON"));
    }
}
