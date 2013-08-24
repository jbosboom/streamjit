package edu.mit.streamjit.test.apps.filterbank6;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.DuplicateSplitter;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.concurrent.ConcurrentStreamCompiler;
import edu.mit.streamjit.impl.distributed.DistributedStreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;

/**
 * Rewritten StreamIt's asplos06 benchmarks. Refer
 * STREAMIT_HOME/apps/benchmarks/asplos06/filterbank/streamit/FilterBank6.str
 * for original implementations. Each StreamIt's language constructs (i.e.,
 * pipeline, filter and splitjoin) are rewritten as classes in StreamJit.
 *
 * @author Sumanan sumanan@mit.edu
 * @since Mar 14, 2013
 */
public class FilterBank6 {

	public static void main(String[] args) throws InterruptedException {
		FilterBank6Kernel kernel = new FilterBank6Kernel();

		Input.ManualInput<Integer> input = Input.createManualInput();
		Output.ManualOutput<Void> output = Output.createManualOutput();

		StreamCompiler sc = new DebugStreamCompiler();
		//StreamCompiler sc = new ConcurrentStreamCompiler(4);
		// StreamCompiler sc = new DistributedStreamCompiler(2);

		CompiledStream stream = sc.compile(kernel, input, output);
		for (int i = 0; i < 1000;) {
			if (input.offer(i)) {
				// System.out.println("Offer success " + i);
				i++;
			} else {
				// System.out.println("Offer failed " + i);
				Thread.sleep(10);
			}
		}
		// Thread.sleep(10000);
		input.drain();
		while (!stream.isDrained())
			;
	}

	/**
	 * FIXME: Actual pipeline is "void->void pipeline FilterBank6". This is a
	 * generic filter bank that decomposes an incoming stream into M frequency
	 * bands. It then performs some processing on them (the exact processing is
	 * yet to be determined, and then reconstructs them.
	 **/
	public static class FilterBank6Kernel extends Pipeline<Integer, Void> {
		public FilterBank6Kernel() {
			add(new DataSource());
			// add FileReader<float>("../input/input");
			add(new FilterBankPipeline(8));
			add(new FloatPrinter());
			// add FileWriter<float>("FilterBank6.out");
		}
	}

	/**
	 * Total FilterBank structure -- the splitjoin and the final adder.
	 **/
	private static class FilterBankPipeline extends Pipeline<Float, Float> {
		FilterBankPipeline(int M) {
			add(new FilterBankSplitJoin(M));
			add(new Adder(M));
		}
	}

	/**
	 * Filterbank splitjoin (everything before the final adder. )
	 **/
	private static class FilterBankSplitJoin extends Splitjoin<Float, Float> {

		FilterBankSplitJoin(int M) {
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
	private static class ProcessingPipeline extends Pipeline<Float, Float> {

		ProcessingPipeline(int M, int i) {

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

	/* This is (obviously) the data source. */
	private static class DataSource extends Filter<Integer, Float> {
		public DataSource() {
			super(1, 1);
		}

		int n = 0;
		float w1 = (float) (Math.PI / 10);
		float w2 = (float) (Math.PI / 20);
		float w3 = (float) (Math.PI / 30);

		public void work() {
			// FIXME:
			pop(); // As current implementation has no support to fire the
			// streamgraph with void element, we offer the graph with
			// random values and just pop out here.
			push((float) (Math.cos(w1 * n) + Math.cos(w2 * n) + Math
					.cos(w3 * n)));
			n++;
		}
	}

	/* this is the filter that we are processing the sub bands with. */
	private static class ProcessFilter extends Filter<Float, Float> {
		ProcessFilter(int order) {
			super(1, 1);
		}

		public void work() {
			push(pop());
		}
	}

	/**
	 * A simple adder which takes in N items and pushes out the sum of them.
	 **/
	private static class Adder extends Filter<Float, Float> {
		int N;

		Adder(int N) {
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
	private static class BandPassFilter extends Pipeline<Float, Float> {
		BandPassFilter(float gain, float ws, float wp, int numSamples) {
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
	private static class BandStopFilter extends Pipeline<Float, Float> {

		BandStopFilter(float gain, float wp, float ws, int numSamples) {
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
	private static class Compressor extends Filter<Float, Float> {
		int M;

		Compressor(int M) {
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
	private static class Expander extends Filter<Float, Float> {
		int L;

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
	 * Simple sink that just prints the data that is fed to it.
	 **/
	private static class FloatPrinter extends Filter<Float, Void> {
		public FloatPrinter() {
			super(1, 0);
		}

		public void work() {
			System.out.println(pop());
		}
	}

	/**
	 * Simple StreamIt filter that simply absorbs floating point numbers without
	 * printing them.
	 **/
	private static class FloatSink extends Filter<Float, Void> {
		public FloatSink() {
			super(1, 0);
		}

		public void work() {
			pop();
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
	private static class HighPassFilter extends Filter<Float, Float> {
		float[] h;
		float g;
		float ws;
		int N;

		HighPassFilter(float g, float ws, int N) {
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
	private static class LowPassFilter extends Filter<Float, Float> {
		float[] h;
		float g;
		float cutoffFreq;
		int N;

		LowPassFilter(float g, float cutoffFreq, int N) {
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
