/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.mit.streamjit.test.apps.fmradio;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.DuplicateSplitter;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.test.SuppliedBenchmark;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Datasets;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler;
import edu.mit.streamjit.test.Benchmark.Dataset;
import edu.mit.streamjit.test.BenchmarkProvider;
import edu.mit.streamjit.test.Benchmarker;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 11/8/2012
 */
public final class FMRadio {
	private FMRadio() {}

	public static void main(String[] args) throws InterruptedException {
		Benchmarker.runBenchmarks(new FMRadioBenchmarkProvider(), new DebugStreamCompiler()).get(0).print(System.out);
	}

	@ServiceProvider(BenchmarkProvider.class)
	public static final class FMRadioBenchmarkProvider implements BenchmarkProvider {
		@Override
		public Iterator<Benchmark> iterator() {
			Path path = Paths.get("data/fmradio.in");
			Input<Float> input = Input.fromBinaryFile(path, Float.class, ByteOrder.LITTLE_ENDIAN);
			input = Datasets.nCopies(1, input);
			Dataset dataset = new Dataset(path.getFileName().toString(), (Input)input
					//use (7, 128) for verification
//					, (Supplier)Suppliers.ofInstance((Input)Input.fromBinaryFile(Paths.get("/home/jbosboom/streamit/streams/apps/benchmarks/asplos06/fm/streamit/FMRadio5.out"), Float.class, ByteOrder.LITTLE_ENDIAN))
			);
			int[][] bandsTaps = {
				{7, 128},
				{11, 64},
				{5, 64},
				{7, 64},
				{9, 64},
				{5, 128},
				{9, 128},
			};
			ImmutableList.Builder<Benchmark> builder = ImmutableList.builder();
			for (int[] p : bandsTaps)
				builder.add(new SuppliedBenchmark(String.format("FMRadio %d, %d", p[0], p[1]),
						FMRadioCore.class, ImmutableList.of(p[0], p[1]),
						dataset));
			return builder.build().iterator();
		}
	}

	private static final class LowPassFilter extends Filter<Float, Float> {
		private final float rate, cutoff;
		private final int taps, decimation;
		private final float[] coeff;

		private LowPassFilter(float rate, float cutoff, int taps, int decimation) {
			super(1 + decimation, 1, taps);
			this.rate = rate;
			this.cutoff = cutoff;
			this.taps = taps;
			this.decimation = decimation;
			this.coeff = new float[taps];
			int i;
			float m = taps - 1;
			float w = (float) (2 * Math.PI * cutoff / rate);
			for (i = 0; i < taps; i++)
				if (i - m / 2 == 0)
					coeff[i] = (float) (w / Math.PI);
				else
					coeff[i] = (float) (Math.sin(w * (i - m / 2)) / Math.PI
							/ (i - m / 2) * (0.54 - 0.46 * Math.cos(2 * Math.PI
							* i / m)));
		}

		@Override
		public void work() {
			float sum = 0;
			for (int i = 0; i < taps; i++)
				sum += peek(i) * coeff[i];
			push(sum);
			for (int i = 0; i < decimation; i++)
				pop();
			pop();
		}
	}

	private static final class Subtractor extends Filter<Float, Float> {
		private Subtractor() {
			super(2, 1, 0);
		}

		@Override
		public void work() {
			float a = pop(), b = pop();
			push(b - a);
		}
	}

	// Inlined into BandPassFilter constructor, since it isn't used elsewhere.
	// private static class BPFCore extends Splitjoin<Float, Float> {
	// public <T extends Object, U extends Object> BPFCore(float rate, float
	// low, float high, int taps) {
	// super(new DuplicateSplitter<Float>(), new RoundrobinJoiner<Float>(),
	// new LowPassFilter(rate, low, taps, 0),
	// new LowPassFilter(rate, high, taps, 0));
	// }
	// }

	private static final class BandPassFilter extends Pipeline<Float, Float> {
		private BandPassFilter(float rate, float low, float high, int taps) {
			// The splitjoin is BPFCore in the StreamIt source.
			super(new Splitjoin<>(
					new DuplicateSplitter<Float>(),
					new RoundrobinJoiner<Float>(),
					new LowPassFilter(rate, low, taps, 0),
					new LowPassFilter(rate, high, taps, 0)),
				new Subtractor());
		}
	}

	private static final class Amplifier extends Filter<Float, Float> {
		private final float k;

		private Amplifier(float k) {
			super(1, 1, 0);
			this.k = k;
		}

		@Override
		public void work() {
			push(pop() * k);
		}
	}

	private static final class Equalizer extends Pipeline<Float, Float> {
		private final float rate;
		private final int bands;
		private final float[] cutoffs, gains;
		private final int taps;

		private Equalizer(float rate, final int bands, float[] cutoffs, float[] gains,
				int taps) {
			this.rate = rate;
			this.bands = bands;
			this.cutoffs = cutoffs;
			this.gains = gains;
			this.taps = taps;

			if (cutoffs.length != bands || gains.length != bands)
				throw new IllegalArgumentException();

			Splitjoin<Float, Float> eqSplit = new Splitjoin<>(
					new DuplicateSplitter<Float>(),
					new RoundrobinJoiner<Float>());
			for (int i = 1; i < bands; ++i)
				eqSplit.add(new Pipeline<Float, Float>(new BandPassFilter(rate,
						cutoffs[i - 1], cutoffs[i], taps), new Amplifier(
						gains[i])));
			add(eqSplit);

			add(new Filter<Float, Float>(bands - 1, 1) {
				@Override
				public void work() {
					float sum = 0;
					for (int i = 0; i < bands - 1; ++i)
						sum += pop();
					push(sum);
				}
			});
		}
	}

	private static final class FMDemodulator extends Filter<Float, Float> {
		private final float gain;

		private FMDemodulator(float sampRate, float max, float bandwidth) {
			this((float) (max * (sampRate / (bandwidth * Math.PI))));
		}

		private FMDemodulator(float gain) {
			super(1, 1, 2);
			this.gain = gain;
		}

		@Override
		public void work() {
			float temp = peek(0) * peek(1);
			temp = (float) (gain * Math.atan(temp));
			pop();
			push(temp);
		}
	}

	public static final class FMRadioCore extends Pipeline<Float, Float> {
		private static final float samplingRate = 250000000; // 250 MHz sampling
																// rate is
																// sensible
		private static final float cutoffFrequency = 108000000; // guess...
																// doesn't FM
																// freq max at
																// 108 Mhz?
		private static final float maxAmplitude = 27000;
		private static final float bandwidth = 10000;
		// determine where equalizer cuts. Note that <eqBands> is the
		// number of CUTS; there are <eqBands>-1 bands, with parameters
		// held in slots 1..<eqBands> of associated arrays.
		private static final float low = 55;
		private static final float high = 1760;

		public FMRadioCore() {
			this(7, 128);
		}

		public FMRadioCore(int bands, int taps) {
			super(makeElements(bands, taps));
		}

		private static List<OneToOneElement<Float, Float>> makeElements(int bands, int taps) {
			float[] eqCutoff = new float[bands];
			float[] eqGain = new float[bands];
			for (int i = 0; i < bands; i++)
				// have exponentially spaced cutoffs
				eqCutoff[i] = (float) Math.exp(i
						* (Math.log(high) - Math.log(low)) / (bands - 1)
						+ Math.log(low));

			// first gain doesn't really correspond to a band
			eqGain[0] = 0;
			for (int i = 1; i < bands; i++) {
				// the gain grows linearly towards the center bands
				float val = (((float) (i - 1)) - (((float) (bands - 2)) / 2.0f)) / 5.0f;
				eqGain[i] = val > 0 ? 2.0f - val : 2.0f + val;
			}

			return Arrays.asList(
					new LowPassFilter(samplingRate, cutoffFrequency, taps, 4),
					new FMDemodulator(samplingRate,	maxAmplitude, bandwidth),
					new Equalizer(samplingRate, bands, eqCutoff, eqGain, taps));
		}
	}
}
