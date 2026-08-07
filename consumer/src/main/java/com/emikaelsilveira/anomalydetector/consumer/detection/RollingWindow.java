package com.emikaelsilveira.anomalydetector.consumer.detection;

/**
 * Fixed-capacity FIFO of primitive samples backed by a ring buffer.
 *
 * <p>Statistics are recomputed in two full passes rather than maintained incrementally. At N &le; 100 the
 * arithmetic disappears next to JSON deserialisation, and the O(1) alternative — subtracting a departing
 * value from a running sum of squares — suffers catastrophic cancellation and can drive the variance
 * negative, producing {@code NaN} from {@code sqrt}. See ADR 0003.
 *
 * <p>Not thread-safe: access is serialized by the single listener container.
 */
final class RollingWindow {

    private final double[] values;
    private int oldest;
    private int size;

    RollingWindow(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.values = new double[capacity];
    }

    void add(double value) {
        if (size == values.length) {
            values[oldest] = value;
            oldest = wrap(oldest + 1);
            return;
        }
        values[wrap(oldest + size)] = value;
        size++;
    }

    int size() {
        return size;
    }

    double mean() {
        double total = 0.0d;
        for (int offset = 0; offset < size; offset++) {
            total += valueAt(offset);
        }
        return total / size;
    }

    double sampleStandardDeviation() {
        return sampleStandardDeviation(mean());
    }

    /** Bessel-corrected (n-1) deviation; the window is a sample of the process, not the population. */
    double sampleStandardDeviation(double mean) {
        if (size < 2) {
            return Double.NaN;
        }

        double squaredDeviationTotal = 0.0d;
        for (int offset = 0; offset < size; offset++) {
            double deviation = valueAt(offset) - mean;
            squaredDeviationTotal += deviation * deviation;
        }
        return Math.sqrt(squaredDeviationTotal / (size - 1));
    }

    private double valueAt(int offset) {
        return values[wrap(oldest + offset)];
    }

    private int wrap(int index) {
        return index < values.length ? index : index - values.length;
    }
}
