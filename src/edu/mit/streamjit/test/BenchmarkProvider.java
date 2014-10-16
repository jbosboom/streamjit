package edu.mit.streamjit.test;

/**
 * A BenchmarkProvider provides benchmarks, to make parameterized sanity tests
 * less verbose by returning Benchmark instances rather than one class per test.
 * <p/>
 * The APP, SANITY and REGRESSION attributes for benchmarks returned by a
 * BenchmarkProvider are based both the provider and benchmark classes.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 8/22/2013
 */
public interface BenchmarkProvider extends Iterable<Benchmark> {
}
