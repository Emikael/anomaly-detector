package com.emikaelsilveira.anomalydetector.producer.generation;

/**
 * Everything about the shape of the synthetic signal, separated from the three collaborators that
 * make it observable ({@link java.util.Random}, {@link java.time.Clock}, the id supplier).
 *
 * <p>Grouped into three sub-profiles rather than one flat carrier: a flat record would still hand
 * the generator eight positional doubles, which is the same call-site guesswork the parameter object
 * was meant to remove. Nesting makes {@code new AnomalyProfile(0.02, 8.0, 12.0)} read as a
 * probability and a sigma band instead of three anonymous numbers.
 *
 * <p>Validation lives on {@code ProducerProperties}, not here: these records describe a generator
 * configuration that tests construct directly with deliberately extreme values.
 */
public record GenerationProfile(
        NoiseProfile noise,
        AnomalyProfile anomaly,
        LevelShiftProfile levelShift
) {

    /** The Gaussian baseline the signal walks around, before any anomaly or regime change. */
    public record NoiseProfile(double mean, double stddev) {
    }

    /**
     * How often a spike replaces the baseline, and how far out it lands. Both bounds are multiples
     * of {@link NoiseProfile#stddev()}, so the injected magnitude scales with the noise rather than
     * being an absolute offset the consumer's z-score could not interpret.
     */
    public record AnomalyProfile(double probability, double sigmaMin, double sigmaMax) {
    }

    /**
     * A one-off regime change: at {@code atSequence} the mean jumps by {@code sigma} standard
     * deviations and stays there, so the consumer's rolling window has to re-baseline.
     */
    public record LevelShiftProfile(boolean enabled, long atSequence, double sigma) {

        /**
         * A profile that never fires. The sequence and sigma are unreachable rather than merely
         * unused, so a caller reading {@code disabled()} cannot mistake them for live defaults.
         */
        public static LevelShiftProfile disabled() {
            return new LevelShiftProfile(false, Long.MAX_VALUE, 0.0);
        }
    }
}
