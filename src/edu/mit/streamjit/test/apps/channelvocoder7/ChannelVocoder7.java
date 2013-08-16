package edu.mit.streamjit.test.apps.channelvocoder7;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.DuplicateSplitter;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StatefulFilter;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.WeightedRoundrobinJoiner;
import edu.mit.streamjit.impl.concurrent.ConcurrentStreamCompiler;
import edu.mit.streamjit.impl.distributed.DistributedStreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;

/**
 * Rewritten StreamIt's asplos06 benchmarks. Refer STREAMIT_HOME/apps/benchmarks/asplos06/channelvocoder/streamit/ChannelVocoder7.str
 * for original implementations. Each StreamIt's language consturcts (i.e., pipeline, filter and splitjoin) are rewritten as classes in
 * StreamJit.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Mar 12, 2013
 */
public class ChannelVocoder7 {
	
	public static void main(String[] args) throws InterruptedException {
		ChannelVocoder7Kernel kernel = new ChannelVocoder7Kernel();
		StreamCompiler sc = new DebugStreamCompiler();
		//StreamCompiler sc = new ConcurrentStreamCompiler(2);
		//StreamCompiler sc = new DistributedStreamCompiler(2);
		CompiledStream<Integer, Void> stream = sc.compile(kernel);
		for (int i = 0; i < 10000;) {
			if (stream.offer(i))
			{
				//System.out.println("Offer success " + i);
				i++;
			}
			else
			{
				//System.out.println("Offer failed " + i);
				Thread.sleep(10);
			}
		}
		//Thread.sleep(20000);
		stream.drain();
		while(!stream.isDrained());
	}

	/**
	 * Represents "void->void pipeline ChannelVocoder7". FIXME: we need void->void pipeline, FileReader<float> and FileWriter<float> to
	 * represents exact implementation.
	 */
	 
	 /** This is a channel vocoder as described in 6.555 Lab 2. It's salient features are a filterbank each of which contains a
	 * decimator after a bandpass filter.
	 * 
	 * Sampling Rate is 8000 Hz. First the signal is conditioned using a lowpass filter with cutoff at 5000 Hz. Then the signal is
	 * "center clipped" which basically means that very high and very low values are removed.
	 * 
	 * Then, the signal is sent both to a pitch detector and to a filter bank with 200 Hz wide windows (18 overall)
	 * 
	 * Thus, each output is the combination of 18 band envelope values from the filter bank and a single pitch detector value. This
	 * value is either the pitch if the sound was voiced or 0 if the sound was unvoiced.
	 **/
	public static class ChannelVocoder7Kernel extends Pipeline<Integer, Void> {

		public ChannelVocoder7Kernel() {
			add(new DataSource());
			// add FileReader<float>("../input/input");
			// low pass filter to filter out high freq noise
			add(new LowPassFilter(1, (float) ((2 * Math.PI * 5000) / 8000), 64));
			add(new MainSplitjoin());
			add(new FloatPrinter());
			// add FileWriter<float>("ChannelVocoder7.out");
		}
	}

	/**
	 * This class is just a wrapper so that we don't have anonymous inner classes.
	 **/
	private static class MainSplitjoin extends Splitjoin<Float, Float> {
		int PITCH_WINDOW = 100; // the number of samples to base the pitch
								// detection on
		int DECIMATION = 50; // decimation factor
		int NUM_FILTERS = 16; // 18;

		MainSplitjoin() {
			super(new DuplicateSplitter<Float>(), new WeightedRoundrobinJoiner<Float>(1, 16)); // FIXME
																								// ,
																								// RoundrobinJoiner
																								// can't
																								// be
																								// NUM_FILTERS
																								// b/c
																								// const
																								// prop
																								// didn't
																								// work
			add(new PitchDetector(PITCH_WINDOW, DECIMATION));
			add(new VocoderFilterBank(NUM_FILTERS, DECIMATION));
		}

	}

	/**
	 * FIXME: void->float filter DataSource, but here Integer->Float /
	 * 
	 * /** a simple data source.
	 **/
	private static class DataSource extends StatefulFilter<Integer, Float> {
		int SIZE = 11;
		int index;
		float[] x;

		DataSource() {
			super(1, 1);
			this.x = new float[SIZE];
			init();
		}

		private void init() {
			index = 0;
			x[0] = -0.70867825f;
			x[1] = 0.9750938f;
			x[2] = -0.009129746f;
			x[3] = 0.28532153f;
			x[4] = -0.42127264f;
			x[5] = -0.95795095f;
			x[6] = 0.68976873f;
			x[7] = 0.99901736f;
			x[8] = -0.8581795f;
			x[9] = 0.9863592f;
			x[10] = 0.909825f;
		}

		public void work() {
			/**
			 * TODO: Remove this pop() once void->float fix is done.
			 */
			pop(); // As current implementation has no support to fire the
			// streamgraph with void element, we offer the graph with
			// random values and just pop out here.
			push(this.x[this.index]);
			this.index = (this.index + 1) % this.SIZE;
		}
	}

	/**
	 * Pitch detector.
	 **/
	private static class PitchDetector extends Pipeline<Float, Float> {

		PitchDetector(int winsize, int decimation) {
			add(new CenterClip());
			add(new CorrPeak(winsize, decimation));
		}
	}

	/** The channel vocoder filterbank. **/
	private static class VocoderFilterBank extends Splitjoin<Float, Float> {
		VocoderFilterBank(int N, int decimation) {
			super(new DuplicateSplitter<Float>(), new RoundrobinJoiner<Float>());
			for (int i = 0; i < N; i++) {
				add(new FilterDecimate(i, decimation));
			}
		}
	}

	/**
	 * A channel of the vocoder filter bank -- has a band pass filter centered at i*200 Hz followed by a decimator with decimation rate
	 * of decimation.
	 **/
	private static class FilterDecimate extends Pipeline<Float, Float> {
		FilterDecimate(int i, int decimation) {
			// add VocoderBandPassFilter(i, 64); // 64 tap filter
			add(new BandPassFilter(2, 400 * i, 400 * (i + 1), 64));
			add(new Compressor(decimation));
		}
	}

	/**
	 * This filter "center clips" the input value so that it is always within the range of -.75 to .75
	 **/
	private static class CenterClip extends Filter<Float, Float> {
		float MIN = -0.75f;
		float MAX = 0.75f;

		public CenterClip() {
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
	 * This filter calculates the autocorrelation of the next winsize elements and then chooses the max peak. If the max peak is under
	 * a threshold we output a zero. If the max peak is above the threshold, we simply output its value.
	 **/
	private static class CorrPeak extends Filter<Float, Float> {
		int winsize;
		int decimation;

		float THRESHOLD = 0.07f;

		CorrPeak(int winsize, int decimation) {
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
	 * This is a bandpass filter with the rather simple implementation of a low pass filter cascaded with a high pass filter. The
	 * relevant parameters are: end of stopband=ws and end of passband=wp, such that 0<=ws<=wp<=pi gain of passband and size of window
	 * for both filters. Note that the high pass and low pass filters currently use a rectangular window.
	 */
	private static class BandPassFilter extends Pipeline<Float, Float> {
		BandPassFilter(float gain, float ws, float wp, int numSamples) {
			add(new LowPassFilter(1, wp, numSamples));
			add(new HighPassFilter(gain, ws, numSamples));
		}
	}

	/*
	 * This is a bandstop filter with the rather simple implementation of a low pass filter cascaded with a high pass filter. The
	 * relevant parameters are: end of passband=wp and end of stopband=ws, such that 0<=wp<=ws<=pi gain of passband and size of window
	 * for both filters. Note that the high pass and low pass filters currently use a rectangular window.
	 * 
	 * We take the signal, run both the low and high pass filter separately and then add the results back together.
	 */
	private static class BandStopFilter extends Pipeline<Float, Float> {

		BandStopFilter(float gain, float wp, float ws, int numSamples) {
			Splitjoin<Float, Float> sp1 = new Splitjoin<>(new DuplicateSplitter<Float>(), new RoundrobinJoiner<Float>());
			sp1.add(new LowPassFilter(gain, wp, numSamples));
			sp1.add(new HighPassFilter(gain, ws, numSamples));
			add(sp1);
			/* sum the two outputs together. */
			add(new Adder(2));
		}
	}

	/**
	 * This filter compresses the signal at its input by a factor M. Eg it inputs M samples, and only outputs the first sample.
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
	 * This filter expands the input by a factor L. Eg in takes in one sample and outputs L samples. The first sample is the input and
	 * the rest of the samples are zeros.
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
	 * Simple StreamIt filter that simply absorbs floating point numbers without printing them.
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
	 * Simple FIR high pass filter with gain=g, stopband ws(in radians) and N samples.
	 * 
	 *  Eg
	 *                 ^ H(e^jw)
	 *                 |
	 *     --------    |    -------
	 *     |      |    |    |     |
	 *     |      |    |    |     |
	 *    <-------------------------> w
	 *                   pi-wc pi pi+wc
	 * 
	 * This implementation is a FIR filter is a rectangularly windowed sinc function (eg sin(x)/x) multiplied by e^(j*pi*n)=(-1)^n,
	 * which is the optimal FIR high pass filter in mean square error terms.
	 * 
	 * Specifically, h[n] has N samples from n=0 to (N-1) such that h[n] = (-1)^(n-N/2) * sin(cutoffFreq*pi*(n-N/2))/(pi*(n-N/2)).
	 * where cutoffFreq is pi-ws and the field h holds h[-n].
	 */
	private static class HighPassFilter extends Filter<Float, Float> {
		float g;
		float ws;
		int N;

		float[] h;

		HighPassFilter(float g, float ws, int N) {
			super(1, 1, N);
			h = new float[N];
			this.g = g;
			this.ws = ws;
			this.N = N;
			init();
		}

		/*
		 * since the impulse response is symmetric, I don't worry about reversing h[n].
		 */
		private void init() {
			int OFFSET = N / 2;
			float cutoffFreq = (float) (Math.PI - ws);
			for (int i = 0; i < N; i++) {
				int idx = i + 1;
				/*
				 * flip signs every other sample (done this way so that it gets array destroyed)
				 */
				int sign = ((i % 2) == 0) ? 1 : -1;
				// generate real part
				if (idx == OFFSET)
					/*
					 * take care of div by 0 error (lim x->oo of sin(x)/x actually equals 1)
					 */
					h[i] = (float) (sign * g * cutoffFreq / Math.PI);
				else
					h[i] = (float) (sign * g * Math.sin(cutoffFreq * (idx - OFFSET)) / (Math.PI * (idx - OFFSET)));
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
	 * Simple FIR low pass filter with gain=g, wc=cutoffFreq(in radians) and N samples.
	 * Eg:
	 *                 ^ H(e^jw)
	 *                 |
	 *          ---------------
	 *          |      |      |
	 *          |      |      |
	 *    <-------------------------> w
	 *         -wc            wc
	 * 
	 * This implementation is a FIR filter is a rectangularly windowed sinc function (eg sin(x)/x), which is the optimal FIR low pass
	 * filter in mean square error terms.
	 * 
	 * Specifically, h[n] has N samples from n=0 to (N-1) such that h[n] = sin(cutoffFreq*pi*(n-N/2))/(pi*(n-N/2)). and the field h
	 * holds h[-n].
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
		 * since the impulse response is symmetric, I don't worry about reversing h[n].
		 */
		private void init() {
			int OFFSET = N / 2;
			for (int i = 0; i < N; i++) {
				int idx = i + 1;
				// generate real part
				if (idx == OFFSET)
					/*
					 * take care of div by 0 error (lim x->oo of sin(x)/x actually equals 1)
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
