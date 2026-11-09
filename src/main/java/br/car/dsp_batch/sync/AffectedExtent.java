package br.car.dsp_batch.sync;

import java.util.Locale;

/**
 * One bounding box in EPSG:3857, stored as {@code minx,miny,maxx,maxy}.
 */
public final class AffectedExtent {

    private AffectedExtent() {
    }

    public static String format(double minX, double minY, double maxX, double maxY) {
        return String.format(Locale.US, "%.6f,%.6f,%.6f,%.6f", minX, minY, maxX, maxY);
    }

    /**
     * Smallest box that contains both extents. A blank side is ignored.
     */
    public static String union(String left, String right) {
        if (left == null || left.isBlank()) {
            return right;
        }
        if (right == null || right.isBlank()) {
            return left;
        }
        double[] a = parse(left);
        double[] b = parse(right);
        return format(
                Math.min(a[0], b[0]),
                Math.min(a[1], b[1]),
                Math.max(a[2], b[2]),
                Math.max(a[3], b[3]));
    }

    private static double[] parse(String extent) {
        String[] parts = extent.split(",");
        if (parts.length != 4) {
            throw new IllegalArgumentException("extent must be minx,miny,maxx,maxy");
        }
        double[] values = new double[4];
        for (int i = 0; i < 4; i++) {
            values[i] = Double.parseDouble(parts[i].trim());
        }
        return values;
    }
}
