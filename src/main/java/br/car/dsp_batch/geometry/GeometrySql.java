package br.car.dsp_batch.geometry;

/**
 * Shared PostGIS SQL fragments for geometry validation and map-ready 2D output.
 * <p>
 * Exhibition / WMS targets use 2D columns. Geometries with Z or M (e.g. Point Z)
 * are flattened with {@code ST_Force2D}; XY is kept, elevation/measure is dropped.
 * Point, LineString, Polygon and multi variants are accepted (no polygon-only extract).
 */
public final class GeometrySql {

    private GeometrySql() {
    }

    /**
     * Predicate: column is present, valid and not empty.
     *
     * @param geometryColumn SQL identifier of the geometry column (already safe/quoted if needed)
     */
    public static String validNonEmptyPredicate(String geometryColumn) {
        requireGeometryExpression(geometryColumn);
        return "(" + geometryColumn + " IS NOT NULL"
                + " AND NOT ST_IsEmpty(ST_MakeValid(" + geometryColumn + ")))";
    }

    /**
     * Wraps an expression with {@code ST_Force2D} (idempotent for already-2D geometries).
     */
    public static String force2d(String geometryExpression) {
        requireGeometryExpression(geometryExpression);
        return "ST_Force2D(" + geometryExpression + ")";
    }

    /**
     * Reprojects the geometry to the target SRID (same contract for UA, AOI, and layers).
     */
    public static String transform(String geometryExpression, int srid) {
        requireGeometryExpression(geometryExpression);
        if (srid <= 0) {
            throw new IllegalArgumentException("srid must be a positive integer");
        }
        return "public.ST_Transform(" + geometryExpression + ", " + srid + ")";
    }

    /** SRID WGS 84 (lon/lat); required before casting to {@code geography} for geodesic area. */
    public static final int WGS84_SRID = 4326;

    /**
     * Geodesic area in square metres. {@code geography} only accepts lon/lat; geometries in other SRIDs
     * must be transformed first (same pattern as theme SQL in application profiles).
     */
    public static String areaGeographySquareMetres(String geometryExpression) {
        requireGeometryExpression(geometryExpression);
        return "public.ST_Area(" + transform(force2d(geometryExpression), WGS84_SRID) + "::geography)";
    }

    /**
     * EWKB of a 2D geometry (Z/M dropped). The transform that sets the SRID stays outside.
     */
    public static String asEwkb2d(String geometryExpression) {
        return "public.ST_AsEWKB(" + force2d(geometryExpression) + ")";
    }

    /**
     * Geometry in EPSG:3857. Source SRID is applied before the transform.
     */
    public static String inWebMercator(String geometryExpression, int sourceSrid) {
        requireGeometryExpression(geometryExpression);
        if (sourceSrid <= 0) {
            throw new IllegalArgumentException("srid must be a positive integer");
        }
        return transform(
                "ST_SetSRID(" + force2d(geometryExpression) + ", " + sourceSrid + ")",
                3857);
    }

    /** Placeholder: EWKB bind, SRID already inside the bytes. */
    public static String geomFromEwkbParam() {
        return "public.ST_GeomFromEWKB(?)";
    }

    /** Same bind, wrapped so a Polygon fits a MultiPolygon column. */
    public static String multiGeomFromEwkbParam() {
        return "public.ST_Multi(" + geomFromEwkbParam() + ")";
    }

    /**
     * Envelope and centroid from one EWKB bind.
     * Fits {@code INSERT INTO t (cols) SELECT <placeholders>, <this> ON CONFLICT ...}.
     */
    public static String envelopeAndCentroidFromEwkb() {
        return "public.ST_Envelope(g), public.ST_Centroid(g) FROM (SELECT "
                + geomFromEwkbParam() + " AS g) ewkb";
    }

    private static void requireGeometryExpression(String geometryExpression) {
        if (geometryExpression == null || geometryExpression.isBlank()) {
            throw new IllegalArgumentException("geometry expression must not be blank");
        }
    }
}
