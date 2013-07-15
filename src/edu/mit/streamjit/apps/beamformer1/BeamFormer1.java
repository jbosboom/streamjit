package edu.mit.streamjit.apps.beamformer1;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.DuplicateSplitter;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StatefulFilter;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.concurrent.ConcurrentStreamCompiler;
import edu.mit.streamjit.impl.distributed.DistributedStreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;

/**
 * Rewritten StreamIt's asplos06 benchmarks. Refer STREAMIT_HOME/apps/benchmarks/asplos06/beamformer/streamit/BeamFormer1.str for
 * original implementations. Each StreamIt's language constructs (i.e., pipeline, filter and splitjoin) are rewritten as classes in
 * StreamJit.
 * 
 * FIXME: All FileWriter<?> and FileReader<?> are replaced with ?Source and ?Printer respectively.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Mar 8, 2013
 */
public class BeamFormer1 {

	public static void main(String[] args) throws InterruptedException {

		BeamFormer1Kernel core = new BeamFormer1Kernel();
		// StreamCompiler sc = new DebugStreamCompiler();
		// StreamCompiler sc = new ConcurrentStreamCompiler(2);
		StreamCompiler sc = new DistributedStreamCompiler(2);
		CompiledStream<Float, Void> stream = sc.compile(core);
		for (float i = 0; i < 100000; ++i) {
			stream.offer(i); // This offer value i has no effect in the program. As we can not call stream.offer(), just sending
								// garbage value.
			// while ((output = stream.poll()) != null)
			// System.out.println(output);
		}
		stream.drain();
		stream.awaitDraining();
	}

	/**
	 * This class represents "pipeline Beamformer1" in the BeamFormer1.str benchmark. "pipeline Beamformer1" is actually void->void.
	 * But, as StreamJit currently doesn't support void input at source worker, Slightly changed to float->void and filereading is
	 * ignored. TODO: Implement the file reading and writing filters in StreamJit and modify this application to work exactly as the
	 * original BeamFormer1.str
	 * 
	 * @author sumanan
	 */
	public static class BeamFormer1Kernel extends Pipeline<Float, Void> {
		int numChannels = 12;
		int numSamples = 256;
		int numBeams = 4;
		int numCoarseFilterTaps = 64;
		int numFineFilterTaps = 64;

		int coarseDecimationRatio = 1;
		int fineDecimationRatio = 2;
		int numSegments = 1;
		int numPostDec1 = numSamples / coarseDecimationRatio;
		int numPostDec2 = numPostDec1 / fineDecimationRatio;
		int mfSize = numSegments * numPostDec2;
		int pulseSize = numPostDec2 / 2;
		int predecPulseSize = pulseSize * coarseDecimationRatio * fineDecimationRatio;
		int targetBeam = numBeams / 4;
		int targetSample = numSamples / 4;

		int targetSamplePostDec = targetSample / coarseDecimationRatio / fineDecimationRatio;
		float dOverLambda = 0.5f;
		float cfarThreshold = (float) (0.95 * dOverLambda * numChannels * (0.5 * pulseSize));

		public BeamFormer1Kernel() {

			// FIXME: split roundrobin(0); is needed. i.e., null splitter, have no pushes to it's children
			Splitjoin<Float, Float> splitJoin1 = new Splitjoin<>(new RoundrobinSplitter<Float>(), new RoundrobinJoiner<Float>(2));
			for (int i = 0; i < numChannels; i++) {
				splitJoin1.add(new Pipeline<Float, Float>(new InputGenerate(i, numSamples, targetBeam, targetSample, cfarThreshold),
						new BeamFirFilter(numCoarseFilterTaps, numSamples, coarseDecimationRatio), new BeamFirFilter(
								numFineFilterTaps, numPostDec1, fineDecimationRatio)));
			}

			Splitjoin<Float, Float> splitJoin2 = new Splitjoin<>(new DuplicateSplitter<Float>(), new RoundrobinJoiner<Float>());
			for (int i = 0; i < numBeams; i++) {
				splitJoin2.add(new Pipeline<Float, Float>(new BeamForm(i, numChannels), new BeamFirFilter(mfSize, numPostDec2, 1),
						new Magnitude()));
			}

			add(splitJoin1);
			add(splitJoin2);
			add(new FloatPrinter());
		}
	}

	// FIXME: We need to support Filter<Void, ?>.
	// Original InputGenerate is void->float.
	private static class InputGenerate extends StatefulFilter<Float, Float> {
		private final int myChannel;
		private final int numberOfSamples;
		private final int tarBeam;
		private final int targetSample;
		private final float thresh;
		private int curSample;
		private final boolean holdsTarget;

		public InputGenerate(int myChannel, int numberOfSamples, int tarBeam, int targetSample, float thresh) {
			super(1, 2, 0);
			this.myChannel = myChannel;
			this.numberOfSamples = numberOfSamples;
			this.tarBeam = tarBeam;
			this.targetSample = targetSample;
			this.thresh = thresh;
			this.curSample = 0;
			this.holdsTarget = (tarBeam == myChannel);
		}

		@Override
		public void work() {
			// FIXME: this pop is just added because current StreamJit doens't support a filter with void input type. Need to support
			// it soon.
			pop(); // As current implementation has no support to fire the
					// streamgraph with void element, we offer the graph with
					// random values and just pop out here.
			if (holdsTarget && (curSample == targetSample)) {
				push((float) Math.sqrt(curSample * myChannel));
				push((float) Math.sqrt(curSample * myChannel) + 1);

			} else {
				push((float) (-Math.sqrt(curSample * myChannel)));
				push((float) (-(Math.sqrt(curSample * myChannel) + 1)));

			}
			curSample++;

			if (curSample >= numberOfSamples) {
				curSample = 0;
			}
		}
	}

	private static class FloatPrinter extends Filter<Float, Void> {

		public FloatPrinter() {
			super(1, 0);
		}

		@Override
		public void work() {
			System.out.println(pop());
		}
	}

	private static class BeamFirFilter extends StatefulFilter<Float, Float> {
		private final int numTaps;
		private final int inputLength;
		private final int decimationRatio;

		private final float[] real_weight;
		private final float[] imag_weight;
		private int numTapsMinusOne;
		private final float[] realBuffer;
		private final float[] imagBuffer;
		private int count;
		private int pos;

		public BeamFirFilter(int numTaps, int inputLength, int decimationRatio) {
			super(2 * decimationRatio, 2, 0);
			this.numTaps = numTaps;
			this.inputLength = inputLength;
			this.decimationRatio = decimationRatio;
			this.real_weight = new float[numTaps];
			this.imag_weight = new float[numTaps];
			this.realBuffer = new float[numTaps];
			this.imagBuffer = new float[numTaps];
			init();
		}

		private void init() {
			int i;
			numTapsMinusOne = numTaps - 1;
			pos = 0;

			for (int j = 0; j < numTaps; j++) {
				int idx = j + 1;
				real_weight[j] = (float) (Math.sin(idx) / ((float) idx));
				imag_weight[j] = (float) (Math.cos(idx) / ((float) idx));
			}
		}

		@Override
		public void work() {
			float real_curr = 0;
			float imag_curr = 0;
			int i;
			int modPos;

			realBuffer[numTapsMinusOne - pos] = pop();

			imagBuffer[numTapsMinusOne - pos] = pop();

			modPos = numTapsMinusOne - pos;
			for (i = 0; i < numTaps; i++) {
				real_curr += realBuffer[modPos] * real_weight[i] + imagBuffer[modPos] * imag_weight[i];
				imag_curr += imagBuffer[modPos] * real_weight[i] + realBuffer[modPos] * imag_weight[i];

				modPos = (modPos + 1) & numTapsMinusOne;
			}

			pos = (pos + 1) & numTapsMinusOne;

			push(real_curr);
			push(imag_curr);

			for (i = 2; i < 2 * decimationRatio; i++) {
				pop();
			}

			count += decimationRatio;

			if (count == inputLength) {
				count = 0;
				pos = 0;
				for (i = 0; i < numTaps; i++) {
					realBuffer[i] = 0;
					imagBuffer[i] = 0;
				}
			}
		}
	}

	private static class Decimator extends Filter<Float, Float> {
		private final int decimationFactor;

		public Decimator(int decimationFactor) {
			super(2 * decimationFactor, 2);
			this.decimationFactor = decimationFactor;
		}

		@Override
		public void work() {
			push(pop());
			push(pop());
			for (int i = 1; i < this.decimationFactor; i++) {
				pop();
				pop();
			}
		}
	}

	private static class CoarseBeamFirFilter extends Filter<Float, Float> {
		private final int numTaps;
		private final int inputLength;
		private final int decimationRatio;

		private float[] real_weight;
		private float[] imag_weight;

		public CoarseBeamFirFilter(int numTaps, int inputLength, int decimationRatio) {
			super(2 * inputLength, 2 * inputLength);
			this.numTaps = numTaps;
			this.inputLength = inputLength;
			this.decimationRatio = decimationRatio;
			real_weight = new float[numTaps];
			imag_weight = new float[numTaps];
			init();
		}

		private void init() {
			int i;

			for (int j = 0; j < numTaps; j++) {
				int idx = j + 1;

				real_weight[j] = (float) (Math.sin(idx) / ((float) idx));
				imag_weight[j] = (float) (Math.cos(idx) / ((float) idx));
			}
		}

		@Override
		public void work() {

			int min;
			if (numTaps < inputLength) {
				min = numTaps;
			} else {
				min = inputLength;
			}
			for (int i = 1; i <= min; i++) {
				float real_curr = 0;
				float imag_curr = 0;
				for (int j = 0; j < i; j++) {
					int realIndex = 2 * (i - j - 1);
					int imagIndex = realIndex + 1;
					real_curr += real_weight[j] * peek(realIndex) + imag_weight[j] * peek(imagIndex);
					imag_curr += real_weight[j] * peek(imagIndex) + imag_weight[j] * peek(realIndex);
				}
				push(real_curr);
				push(imag_curr);
			}

			for (int i = 0; i < inputLength - numTaps; i++) {
				pop();
				pop();
				float real_curr = 0;
				float imag_curr = 0;
				for (int j = 0; j < numTaps; j++) {
					int realIndex = 2 * (numTaps - j - 1);
					int imagIndex = realIndex + 1;
					real_curr += real_weight[j] * peek(realIndex) + imag_weight[j] * peek(imagIndex);
					imag_curr += real_weight[j] * peek(imagIndex) + imag_weight[j] * peek(realIndex);
				}
				push(real_curr);
				push(imag_curr);
			}

			for (int i = 0; i < min; i++) {
				pop();
				pop();
			}
		}
	}

	private static class BeamForm extends Filter<Float, Float> {
		int myBeamId;
		int numChannels;
		float[] real_weight;
		float[] imag_weight;

		public BeamForm(int myBeamId, int numChannels) {
			super(2 * numChannels, 2);
			this.myBeamId = myBeamId;
			this.numChannels = numChannels;
			real_weight = new float[numChannels];
			imag_weight = new float[numChannels];
			init();
		}

		private void init() {
			for (int j = 0; j < numChannels; j++) {
				int idx = j + 1;
				real_weight[j] = (float) (Math.sin(idx) / ((float) (myBeamId + idx)));
				imag_weight[j] = (float) (Math.cos(idx) / ((float) (myBeamId + idx)));
			}
		}

		@Override
		public void work() {
			float real_curr = 0;
			float imag_curr = 0;
			for (int i = 0; i < numChannels; i++) {
				float real_pop = pop();
				float imag_pop = pop();

				real_curr += real_weight[i] * real_pop - imag_weight[i] * imag_pop;
				imag_curr += real_weight[i] * imag_pop + imag_weight[i] * real_pop;
			}
			push(real_curr);
			push(imag_curr);
		}
	}

	private static class Magnitude extends Filter<Float, Float> {

		public Magnitude() {
			super(2, 1);
		}

		@Override
		public void work() {
			float f1 = pop();
			float f2 = pop();
			push(mag(f1, f2));
		}

		private float mag(float real, float imag) {
			return (float) Math.sqrt(real * real + imag * imag);
		}

	}

	private static class Detector extends Filter<Float, Float> {
		private int _myBeam;
		private int numSamples;
		private int targetBeam;
		private int targetSample;
		private float cfarThreshold;

		private int curSample;
		private int myBeam;
		private boolean holdsTarget;
		private float thresh;

		public Detector(int _myBeam, int numSamples, int targetBeam, int targetSample, float cfarThresholde) {
			super(1, 1);
			this._myBeam = _myBeam;
			this.numSamples = numSamples;
			this.targetBeam = targetBeam;
			this.targetSample = targetSample;
			this.cfarThreshold = cfarThreshold;
		}

		private void init() {
			curSample = 0;
			holdsTarget = (_myBeam == targetBeam);
			myBeam = _myBeam + 1;
			thresh = 0.1f;
		}

		@Override
		public void work() {
			float inputVal = pop();
			float outputVal;
			if (holdsTarget && targetSample == curSample) {
				if (!(inputVal >= thresh)) {
					outputVal = 0;
				} else {
					outputVal = myBeam;
				}
			} else {
				if (!(inputVal >= thresh)) {
					outputVal = 0;
				} else {
					outputVal = -myBeam;
				}
			}

			outputVal = inputVal;
			// println (outputVal);
			push(outputVal);

			curSample++;

			if (curSample >= numSamples)
				curSample = 0;
		}
	}
}
