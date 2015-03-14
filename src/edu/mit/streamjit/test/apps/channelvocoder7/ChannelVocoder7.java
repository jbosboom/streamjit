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
package edu.mit.streamjit.test.apps.channelvocoder7;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.DuplicateSplitter;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.WeightedRoundrobinJoiner;
import edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmark.Dataset;
import edu.mit.streamjit.test.BenchmarkProvider;
import edu.mit.streamjit.test.Benchmarker;
import edu.mit.streamjit.test.SuppliedBenchmark;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * Rewritten StreamIt's asplos06 benchmarks. Refer
 * STREAMIT_HOME/apps/benchmarks/
 * asplos06/channelvocoder/streamit/ChannelVocoder7.str for original
 * implementations. Each StreamIt's language consturcts (i.e., pipeline, filter
 * and splitjoin) are rewritten as classes in StreamJit.
 *
 * @author Sumanan sumanan@mit.edu
 * @since Mar 12, 2013
 */
@ServiceProvider(BenchmarkProvider.class)
public final class ChannelVocoder7 implements BenchmarkProvider {
	public static void main(String[] args) throws InterruptedException {
		StreamCompiler sc = new Compiler2StreamCompiler().maxNumCores(8).multiplier(32);
		Benchmarker.runBenchmark(Iterables.getLast(new ChannelVocoder7()), sc).get(0).print(System.out);
	}

	@Override
	public Iterator<Benchmark> iterator() {
		Dataset dataset = new Dataset("vocoder.in", (Input)Input.fromBinaryFile(Paths.get("data/vocoder.in"), Float.class, ByteOrder.LITTLE_ENDIAN)
//				, (Supplier)Suppliers.ofInstance((Input)Input.fromBinaryFile(Paths.get("/home/jbosboom/streamit/streams/apps/benchmarks/asplos06/channelvocoder/streamit/ChannelVocoder7.out"), Float.class, ByteOrder.LITTLE_ENDIAN))
				);
		int[][] filtersTaps = {
			{16, 64},
			{4, 64},
			{8, 64},
			{12, 64},
			{4, 128},
			{8, 128},
			{12, 128},
			{16, 128},
			{20, 64},
			{20, 128},
			{24, 64},
			{24, 128},
		};
		ImmutableList.Builder<Benchmark> builder = ImmutableList.builder();
		for (int[] p : filtersTaps)
			builder.add(new SuppliedBenchmark(String.format("ChannelVocoder %d, %d", p[0], p[1]),
					ChannelVocoder7Kernel.class, ImmutableList.of(p[0], p[1]),
					dataset));
		return builder.build().iterator();
	}

	/**
	 * This is a channel vocoder as described in 6.555 Lab 2. It's salient
	 * features are a filterbank each of which contains a decimator after a
	 * bandpass filter.
	 *
	 * Sampling Rate is 8000 Hz. First the signal is conditioned using a lowpass
	 * filter with cutoff at 5000 Hz. Then the signal is "center clipped" which
	 * basically means that very high and very low values are removed.
	 *
	 * Then, the signal is sent both to a pitch detector and to a filter bank
	 * with 200 Hz wide windows (18 overall)
	 *
	 * Thus, each output is the combination of 18 band envelope values from the
	 * filter bank and a single pitch detector value. This value is either the
	 * pitch if the sound was voiced or 0 if the sound was unvoiced.
	 **/
	public static final class ChannelVocoder7Kernel extends Pipeline<Float, Float> {
		public ChannelVocoder7Kernel(int numFilters, int numTaps) {
			// low pass filter to filter out high freq noise
			add(new LowPassFilter(1, (float) ((2 * Math.PI * 5000) / 8000), 64));
			add(new MainSplitjoin(numFilters, numTaps));
		}
	}

	/**
	 * This class is just a wrapper so that we don't have anonymous inner
	 * classes.
	 **/
	private static final class MainSplitjoin extends Splitjoin<Float, Float> {
		private static final int PITCH_WINDOW = 100; // the number of samples to base the pitch
								// detection on
		private static final int DECIMATION = 50; // decimation factor

		private MainSplitjoin(int numFilters, int numTaps) {
			super(new DuplicateSplitter<Float>(),
					new WeightedRoundrobinJoiner<Float>(1, numFilters));
			add(new PitchDetector(PITCH_WINDOW, DECIMATION));
			add(new VocoderFilterBank(numFilters, DECIMATION, numTaps));
		}

	}

	/**
	 * Pitch detector.
	 **/
	private static final class PitchDetector extends Pipeline<Float, Float> {
		private PitchDetector(int winsize, int decimation) {
			add(new CenterClip());
			add(new CorrPeak(winsize, decimation));
		}
	}

	/** The channel vocoder filterbank. **/
	private static final class VocoderFilterBank extends Splitjoin<Float, Float> {
		private VocoderFilterBank(int N, int decimation, int numTaps) {
			super(new DuplicateSplitter<Float>(), new RoundrobinJoiner<Float>());
			for (int i = 0; i < N; i++) {
				add(new FilterDecimate(i, decimation, numTaps));
			}
		}
	}

	/**
	 * A channel of the vocoder filter bank -- has a band pass filter centered
	 * at i*200 Hz followed by a decimator with decimation rate of decimation.
	 **/
	private static final class FilterDecimate extends Pipeline<Float, Float> {
		private FilterDecimate(int i, int decimation, int numTaps) {
			// add VocoderBandPassFilter(i, 64); // 64 tap filter
			add(new BandPassFilter(2, 400 * i, 400 * (i + 1), numTaps));
			add(new Compressor(decimation));
		}
	}

	/**
	 * This filter "center clips" the input value so that it is always within
	 * the range of -.75 to .75
	 **/
	private static final class CenterClip extends Filter<Float, Float> {
		private final float MIN = -0.75f;
		private final float MAX = 0.75f;

		private CenterClip() {
			super(1, 1);
		}

		public void work() {
			float t = pop();
			if (t < MIN) {
				push(MIN);
			} else if (t > MAX) {
				push(MAX);
			} else {
				push(t);
			}
		}
	}

	/**
	 * This filter calculates the autocorrelation of the next winsize elements
	 * and then chooses the max peak. If the max peak is under a threshold we
	 * output a zero. If the max peak is above the threshold, we simply output
	 * its value.
	 **/
	private static final class CorrPeak extends Filter<Float, Float> {
		private static final float THRESHOLD = 0.07f;
		private final int winsize;
		private final int decimation;

		private CorrPeak(int winsize, int decimation) {
			super(decimation, 1, winsize);
			this.winsize = winsize;
			this.decimation = decimation;
		}

		public void work() {
			float[] autocorr = new float[this.winsize]; // auto correlation
			for (int i = 0; i < this.winsize; i++) {
				float sum = 0;
				for (int j = i; j < winsize; j++) {
					sum += peek(i) * peek(j);
				}
				autocorr[i] = sum / winsize;
			}

			// armed with the auto correlation, find the max peak
			// in a real vocoder, we would restrict our attention to
			// the first few values of the auto corr to catch the initial peak
			// due to the fundamental frequency.
			float maxpeak = 0;
			for (int i = 0; i < winsize; i++) {
				if (autocorr[i] > maxpeak) {
					maxpeak = autocorr[i];
				}
			}

			// println("max peak" + maxpeak);

			// output the max peak if it is above the threshold.
			// otherwise output zero;
			if (maxpeak > THRESHOLD) {
				push(maxpeak);
			} else {
				push(0f);
			}
			for (int i = 0; i < decimation; i++) {
				pop();
			}
		}
	}

	/**
	 * A simple adder which takes in N items and pushes out the sum of them.
	 **/
	private static final class Adder extends Filter<Float, Float> {
		private final int N;

		private Adder(int N) {
			super(N, 1);
			this.N = N;
		}

		public void work() {
			float sum = 0;
			for (int i = 0; i < N; i++) {
				sum += pop();
			}
			push(sum);
		}
	}

	/*
	 * This is a bandpass filter with the rather simple implementation of a low
	 * pass filter cascaded with a high pass filter. The relevant parameters
	 * are: end of stopband=ws and end of passband=wp, such that 0<=ws<=wp<=pi
	 * gain of passband and size of window for both filters. Note that the high
	 * pass and low pass filters currently use a rectangular window.
	 */
	private static final class BandPassFilter extends Pipeline<Float, Float> {
		private BandPassFilter(float gain, float ws, float wp, int numSamples) {
			add(new LowPassFilter(1, wp, numSamples));
			add(new HighPassFilter(gain, ws, numSamples));
		}
	}

	/*
	 * This is a bandstop filter with the rather simple implementation of a low
	 * pass filter cascaded with a high pass filter. The relevant parameters
	 * are: end of passband=wp and end of stopband=ws, such that 0<=wp<=ws<=pi
	 * gain of passband and size of window for both filters. Note that the high
	 * pass and low pass filters currently use a rectangular window.
	 *
	 * We take the signal, run both the low and high pass filter separately and
	 * then add the results back together.
	 */
	private static final class BandStopFilter extends Pipeline<Float, Float> {
		private BandStopFilter(float gain, float wp, float ws, int numSamples) {
			Splitjoin<Float, Float> sp1 = new Splitjoin<>(
					new DuplicateSplitter<Float>(),
					new RoundrobinJoiner<Float>());
			sp1.add(new LowPassFilter(gain, wp, numSamples));
			sp1.add(new HighPassFilter(gain, ws, numSamples));
			add(sp1);
			/* sum the two outputs together. */
			add(new Adder(2));
		}
	}

	/**
	 * This filter compresses the signal at its input by a factor M. Eg it
	 * inputs M samples, and only outputs the first sample.
	 **/
	private static final class Compressor extends Filter<Float, Float> {
		private final int M;
		private Compressor(int M) {
			super(M, 1);
			this.M = M;
		}
		public void work() {
			push(pop());
			for (int i = 0; i < (M - 1); i++) {
				pop();
			}
		}
	}

	/**
	 * This filter expands the input by a factor L. Eg in takes in one sample
	 * and outputs L samples. The first sample is the input and the rest of the
	 * samples are zeros.
	 **/
	private static final class Expander extends Filter<Float, Float> {
		private final int L;
		Expander(int L) {
			super(1, L);
			this.L = L;
		}
		public void work() {
			push(pop());
			for (int i = 0; i < (L - 1); i++) {
				push(0f);
			}
		}
	}

	/**
	 * Simple FIR high pass filter with gain=g, stopband ws(in radians) and N
	 * samples.
	 *
	 * Eg
-	 *                 ^ H(e^jw)
-	 *                 |
-	 *     --------    |    -------
-	 *     |      |    |    |     |
-	 *     |      |    |    |     |
-	 *    <-------------------------> w
-	 *                   pi-wc pi pi+wc
	 *
	 * This implementation is a FIR filter is a rectangularly windowed sinc
	 * function (eg sin(x)/x) multiplied by e^(j*pi*n)=(-1)^n, which is the
	 * optimal FIR high pass filter in mean square error terms.
	 *
	 * Specifically, h[n] has N samples from n=0 to (N-1) such that h[n] =
	 * (-1)^(n-N/2) * sin(cutoffFreq*pi*(n-N/2))/(pi*(n-N/2)). where cutoffFreq
	 * is pi-ws and the field h holds h[-n].
	 */
	private static final class HighPassFilter extends Filter<Float, Float> {
		private final float g;
		private final float ws;
		private final int N;
		private final float[] h;

		private HighPassFilter(float g, float ws, int N) {
			super(1, 1, N);
			this.h = new float[N];
			this.g = g;
			this.ws = ws;
			this.N = N;
			init();
		}

		/*
		 * since the impulse response is symmetric, I don't worry about
		 * reversing h[n].
		 */
		private void init() {
			int OFFSET = N / 2;
			float cutoffFreq = (float) (Math.PI - ws);
			for (int i = 0; i < N; i++) {
				int idx = i + 1;
				/*
				 * flip signs every other sample (done this way so that it gets
				 * array destroyed)
				 */
				int sign = ((i % 2) == 0) ? 1 : -1;
				// generate real part
				if (idx == OFFSET)
					/*
					 * take care of div by 0 error (lim x->oo of sin(x)/x
					 * actually equals 1)
					 */
					h[i] = (float) (sign * g * cutoffFreq / Math.PI);
				else
					h[i] = (float) (sign * g
							* Math.sin(cutoffFreq * (idx - OFFSET)) / (Math.PI * (idx - OFFSET)));
			}

		}

		/* implement the FIR filtering operation as the convolution sum. */
		public void work() {
			float sum = 0;
			for (int i = 0; i < N; i++) {
				sum += h[i] * peek(i);
			}
			push(sum);
			pop();
		}
	}

	/**
	 * Simple FIR low pass filter with gain=g, wc=cutoffFreq(in radians) and N
	 * samples.
-	 * Eg:
-	 *                 ^ H(e^jw)
-	 *                 |
-	 *          ---------------
-	 *          |      |      |
-	 *          |      |      |
-	 *    <-------------------------> w
-	 *         -wc            wc
	 *
	 * This implementation is a FIR filter is a rectangularly windowed sinc
	 * function (eg sin(x)/x), which is the optimal FIR low pass filter in mean
	 * square error terms.
	 *
	 * Specifically, h[n] has N samples from n=0 to (N-1) such that h[n] =
	 * sin(cutoffFreq*pi*(n-N/2))/(pi*(n-N/2)). and the field h holds h[-n].
	 */
	private static final class LowPassFilter extends Filter<Float, Float> {
		private final float[] h;
		private final float g;
		private final float cutoffFreq;
		private final int N;

		private LowPassFilter(float g, float cutoffFreq, int N) {
			super(1, 1, N);
			this.h = new float[N];
			this.g = g;
			this.cutoffFreq = cutoffFreq;
			this.N = N;
			init();
		}

		/*
		 * since the impulse response is symmetric, I don't worry about
		 * reversing h[n].
		 */
		private void init() {
			int OFFSET = N / 2;
			for (int i = 0; i < N; i++) {
				int idx = i + 1;
				// generate real part
				if (idx == OFFSET)
					/*
					 * take care of div by 0 error (lim x->oo of sin(x)/x
					 * actually equals 1)
					 */
					h[i] = (float) (g * cutoffFreq / Math.PI);
				else
					h[i] = (float) (g * Math.sin(cutoffFreq * (idx - OFFSET)) / (Math.PI * (idx - OFFSET)));
			}
		}

		/* Implement the FIR filtering operation as the convolution sum. */
		public void work() {
			float sum = 0;
			for (int i = 0; i < N; i++) {
				sum += h[i] * peek(i);
			}
			push(sum);
			pop();
		}
	}
}
