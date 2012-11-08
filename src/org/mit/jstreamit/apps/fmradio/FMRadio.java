package org.mit.jstreamit.apps.fmradio;

import java.util.ArrayList;
import java.util.List;
import org.mit.jstreamit.*;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/8/2012
 */
public class FMRadio {
	public static void main(String[] args) {
		FMRadioCore core = new FMRadioCore();
	}

	private static class LowPassFilter extends Filter<Float, Float> {
		private final float rate, cutoff;
		private final int taps, decimation;
		private final float[] coeff;
		LowPassFilter(float rate, float cutoff, int taps, int decimation) {
			super(1+decimation, 1, taps);
			this.rate = rate;
			this.cutoff = cutoff;
			this.taps = taps;
			this.decimation = decimation;
			this.coeff = new float[taps];
			int i;
			float m = taps - 1;
			float w = (float)(2 * Math.PI * cutoff / rate);
			for (i = 0; i < taps; i++)
				if (i - m / 2 == 0)
					coeff[i] = (float)(w / Math.PI);
				else
					coeff[i] = (float)(Math.sin(w * (i - m / 2)) / Math.PI / (i - m / 2)
							* (0.54 - 0.46 * Math.cos(2 * Math.PI * i / m)));
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

	private static class Subtractor extends Filter<Float, Float> {
		Subtractor() {
			super(2, 1, 0);
		}
		@Override
		public void work() {
			//This is Java; we can depend on pop() ordering.
			push(pop() - pop());
		}
	}

	//Inlined into BandPassFilter constructor, since it isn't used elsewhere.
//	private static class BPFCore extends Splitjoin<Float, Float> {
//		public <T extends Object, U extends Object> BPFCore(float rate, float low, float high, int taps) {
//			super(new DuplicateSplitter<Float>(), new RoundrobinJoiner<Float>(),
//					new LowPassFilter(rate, low, taps, 0),
//					new LowPassFilter(rate, high, taps, 0));
//		}
//	}

	private static class BandPassFilter extends Pipeline<Float, Float> {
		BandPassFilter(float rate, float low, float high, int taps) {
			//The splitjoin is BPFCore in the StreamIt source.
			super(new Splitjoin<>(new DuplicateSplitter<Float>(), new RoundrobinJoiner<Float>(),
					new LowPassFilter(rate, low, taps, 0),
					new LowPassFilter(rate, high, taps, 0)),
				new Subtractor());
		}
	}

	private static class Amplifier extends Filter<Float, Float> {
		private final float k;
		Amplifier(float k) {
			super(1, 1, 0);
			this.k = k;
		}
		@Override
		public void work() {
			push(pop() * k);
		}
	}

	private static class Equalizer extends Pipeline<Float, Float> {
		private final float rate;
		private final int bands;
		private final float[] cutoffs, gains;
		private final int taps;
		Equalizer(float rate, int bands, float[] cutoffs, float[] gains, int taps) {
			super(makeElements(rate, bands, cutoffs, gains, taps));
			this.rate = rate;
			this.bands = bands;
			this.cutoffs = cutoffs;
			this.gains = gains;
			this.taps = taps;
		}

		private static StreamElement[] makeElements(float rate, final int bands, float[] cutoffs, float[] gains, int taps) {
			if (cutoffs.length != bands || gains.length != bands)
				throw new IllegalArgumentException();

			List<StreamElement<Float, Float>> splitjoinBody = new ArrayList<>(bands-1);
			for (int i = 1; i < bands; ++i)
				splitjoinBody.add(new Pipeline<Float, Float>(
						new BandPassFilter(rate, cutoffs[i-1], cutoffs[i], taps),
						new Amplifier(gains[i]))
					);
			Splitjoin<Float, Float> eqSplit = new Splitjoin<>(
					new DuplicateSplitter<>(), new RoundrobinJoiner<>(),
					splitjoinBody.toArray(new StreamElement[0]));

			//This is what an anonymous filter looks like
			Filter summer = new Filter<Float, Float>(bands-1, 1, 0) {
				@Override
				public void work() {
					float sum = 0;
					for (int i = 0; i < bands-1; ++i)
						sum += pop();
					push(sum);
				}
			};

			return new StreamElement[]{eqSplit, summer};
		}
	}

	private static class FMDemodulator extends Filter<Float, Float> {
		float gain;
		FMDemodulator(float sampRate, float max, float bandwidth) {
			super(1, 1, 2);
			this.gain = (float)(max*(sampRate/(bandwidth*Math.PI)));
		}
		@Override
		public void work() {
			float temp = peek(0) * peek(1);
			temp = (float)(gain * Math.atan(temp));
			pop();
			push(temp);
		}
	}

	private static class FMRadioCore extends Pipeline<Float, Float> {
		private static final float samplingRate = 250000000; // 250 MHz sampling rate is sensible
		private static final float cutoffFrequency = 108000000; //guess... doesn't FM freq max at 108 Mhz?
		private static final int numberOfTaps = 64;
		private static final float maxAmplitude = 27000;
		private static final float bandwidth = 10000;
		// determine where equalizer cuts.  Note that <eqBands> is the
		// number of CUTS; there are <eqBands>-1 bands, with parameters
		// held in slots 1..<eqBands> of associated arrays.
		private static final int eqBands = 11;
		private static final float[] eqCutoff = new float[eqBands];
		private static final float[] eqGain = new float[eqBands];
		private static final float low = 55;
		private static final float high = 1760;

		static {
			for (int i = 0; i < eqBands; i++)
				// have exponentially spaced cutoffs
				eqCutoff[i] = (float)Math.exp(i * (Math.log(high) - Math.log(low)) / (eqBands - 1) + Math.log(low));

			// first gain doesn't really correspond to a band
			eqGain[0] = 0;
			for (int i = 1; i < eqBands; i++) {
				// the gain grows linearly towards the center bands
				float val = (((float)(i - 1)) - (((float)(eqBands - 2)) / 2.0f)) / 5.0f;
				eqGain[i] = val > 0 ? 2.0f - val : 2.0f + val;
			}
		}

		FMRadioCore() {
			super(new LowPassFilter(samplingRate, cutoffFrequency, numberOfTaps, 4),
					new FMDemodulator(samplingRate, maxAmplitude, bandwidth),
					new Equalizer(samplingRate, eqBands, eqCutoff, eqGain, numberOfTaps));
		}
	}
}
