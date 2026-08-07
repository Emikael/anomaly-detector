package com.emikaelsilveira.anomalydetector.consumer.detection;

import java.util.ArrayDeque;

final class RollingWindow {

    private final int capacity;
    private final ArrayDeque<Double> values = new ArrayDeque<>();

    RollingWindow(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.capacity = capacity;
    }

    void add(double value) {
        if (values.size() == capacity) {
            values.removeFirst();
        }
        values.addLast(value);
    }

    int size() {
        return values.size();
    }

    double mean() {
        double total = 0.0;
        for (double value : values) {
            total += value;
        }
        return total / values.size();
    }

    double sampleStandardDeviation() {
        int size = values.size();
        if (size < 2) {
            return Double.NaN;
        }

        double mean = mean();
        double squaredDeviationTotal = 0.0;
        for (double value : values) {
            double deviation = value - mean;
            squaredDeviationTotal += deviation * deviation;
        }
        return Math.sqrt(squaredDeviationTotal / (size - 1));
    }
}
