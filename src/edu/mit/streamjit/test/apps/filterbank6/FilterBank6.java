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
package edu.mit.streamjit.test.apps.filterbank6;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.DuplicateSplitter;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;
import edu.mit.streamjit.test.AbstractBenchmark;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmarker;
import edu.mit.streamjit.test.Datasets;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Rewritten StreamIt's asplos06 benchmarks. Refer
 * STREAMIT_HOME/apps/benchmarks/asplos06/filterbank/streamit/FilterBank6.str
 * for original implementations. Each StreamIt's language constructs (i.e.,
 * pipeline, filter and splitjoin) are rewritten as classes in StreamJit.
 *
 * @author Sumanan sumanan@mit.edu
 * @since Mar 14, 2013
 */
public final class FilterBank6 {
	private FilterBank6() {}

	public static void main(String[] args) {
		StreamCompiler sc = new DebugStreamCompiler();
		Benchmarker.runBenchmark(new FilterBankBenchmark(), sc).get(0).print(System.out);
	}

	@ServiceProvider(Benchmark.class)
	public static final class FilterBankBenchmark extends AbstractBenchmark {
		public FilterBankBenchmark() {
			super(dataset());
		}
		private static Dataset dataset() {
			Path path = Paths.get("data/filterbank.in");
			Input<Float> input = Input.fromBinaryFile(path, Float.class, ByteOrder.LITTLE_ENDIAN);
			Input<Float> repeated = Datasets.nCopies(1, input);
			Dataset dataset = new Dataset(path.getFileName().toString(), (Input)repeated
//					, (Supplier)Suppliers.ofInstance((Input)Input.fromBinaryFile(Paths.get("/home/jbosboom/streamit/streams/apps/benchmarks/asplos06/filterbank/streamit/FilterBank6.out"), Float.class, ByteOrder.LITTLE_ENDIAN))
			);
			return dataset;
		}
		@Override
		@SuppressWarnings("unchecked")
		public OneToOneElement<Object, Object> instantiate() {
			return (OneToOneElement)new FilterBankPipeline(8);
		}
	}

	/**
	 * Top-level filterbank structure.
	 **/
	private static final class FilterBankPipeline extends Pipeline<Float, Float> {
		private FilterBankPipeline(int M) {
			add(new FilterBankSplitJoin(M));
			add(new Adder(M));
		}
	}

	/**
	 * Filterbank splitjoin (everything before the final adder. )
	 **/
	private static final class FilterBankSplitJoin extends Splitjoin<Float, Float> {
		private FilterBankSplitJoin(int M) {
			super(new DuplicateSplitter<Float>(), new RoundrobinJoiner<Float>());
			for (int i = 0; i < M; i++) {
				add(new ProcessingPipeline(M, i));
			}
		}
	}

	/**
	 * The main processing pipeline: analysis, downsample, process, upsample,
	 * synthesis. I use simple bandpass filters for the Hi(z) and Fi(z).
	 **/
	private static final class ProcessingPipeline extends Pipeline<Float, Float> {
		private ProcessingPipeline(int M, int i) {
			/* take the subband from i*pi/M to (i+1)*pi/M */
			add(new BandPassFilter(1, (float) (i * Math.PI / M),
					(float) ((i + 1) * Math.PI / M), 128));
			/* decimate by M */
			add(new Compressor(M));

			/* process the subband */
			add(new ProcessFilter(i));

			/* upsample by M */
			add(new Expander(M));
			/* synthesize (eg interpolate) */
			add(new BandStopFilter(M, (float) (i * Math.PI / M),
					(float) ((i + 1) * Math.PI / M), 128));
		}
	}

	/* this is the filter that we are processing the sub bands with. */
	private static final class ProcessFilter extends Filter<Float, Float> {
		private ProcessFilter(int order) {
			super(1, 1);
		}
		public void work() {
			//TODO: shouldn't there be some compute here?
			push(pop());
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
			super(new Splitjoin<>(new DuplicateSplitter<Float>(), new RoundrobinJoiner<Float>(),
						new LowPassFilter(gain, wp, numSamples),
						new HighPassFilter(gain, ws, numSamples)),
					new Adder(2));
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
	 * Eg ^ H(e^jw) | -------- | ------- | | | | | | | | | |
	 * <-------------------------> w pi-wc pi pi+wc
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
		private final float[] h;
		private final float g;
		private final float ws;
		private final int N;
		private HighPassFilter(float g, float ws, int N) {
			super(1, 1, N);
			h = new float[N];
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
	 * samples. Eg: ^ H(e^jw) | --------------- | | | | | |
	 * <-------------------------> w -wc wc
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
			h = new float[N];
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
