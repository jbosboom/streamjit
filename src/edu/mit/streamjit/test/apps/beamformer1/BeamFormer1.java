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
package edu.mit.streamjit.test.apps.beamformer1;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.DuplicateSplitter;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StatefulFilter;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmark.Dataset;
import edu.mit.streamjit.test.Benchmarker;
import edu.mit.streamjit.test.SuppliedBenchmark;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.util.Collections;

/**
 * Rewritten StreamIt's asplos06 benchmarks. Refer
 * STREAMIT_HOME/apps/benchmarks/asplos06/beamformer/streamit/BeamFormer1.str
 * for original implementations. Each StreamIt's language constructs (i.e.,
 * pipeline, filter and splitjoin) are rewritten as classes in StreamJit.
 * @author Sumanan sumanan@mit.edu
 * @since Mar 8, 2013
 */
public final class BeamFormer1 {
	private BeamFormer1() {}

	public static void main(String[] args) throws InterruptedException {
		StreamCompiler sc = new Compiler2StreamCompiler();
		Benchmarker.runBenchmark(new BeamFormerBenchmark(), sc).get(0).print(System.out);
	}

	@ServiceProvider(Benchmark.class)
	public static final class BeamFormerBenchmark extends SuppliedBenchmark {
		//how many dummy timing elements to provide
		private static final int ITEMS = 10_000_000;
		public BeamFormerBenchmark() {
			super("Beamformer", BeamFormer1Kernel.class, new Dataset(""+ITEMS, (Input)Input.fromIterable(Collections.nCopies(ITEMS, (byte)0))
//					, (Supplier)Suppliers.ofInstance((Input)Input.fromBinaryFile(Paths.get("/home/jbosboom/streamit/streams/apps/benchmarks/asplos06/beamformer/streamit/BeamFormer1.out"), Float.class, ByteOrder.LITTLE_ENDIAN))
			));
		}
	}

	/**
	 * Takes dummy timing input (would run forever otherwise).  The type is
	 * irrelevant, so we use Byte to permit unboxing.
	 * @author sumanan
	 */
	public static final class BeamFormer1Kernel extends Pipeline<Byte, Float> {
		private static final int numChannels = 12;
		private static final int numSamples = 256;
		private static final int numBeams = 4;
		private static final int numCoarseFilterTaps = 64;
		private static final int numFineFilterTaps = 64;
		private static final int coarseDecimationRatio = 1;
		private static final int fineDecimationRatio = 2;
		private static final int numSegments = 1;
		private static final int numPostDec1 = numSamples / coarseDecimationRatio;
		private static final int numPostDec2 = numPostDec1 / fineDecimationRatio;
		private static final int mfSize = numSegments * numPostDec2;
		private static final int pulseSize = numPostDec2 / 2;
		private static final int predecPulseSize = pulseSize * coarseDecimationRatio * fineDecimationRatio;
		private static final int targetBeam = numBeams / 4;
		private static final int targetSample = numSamples / 4;
		private static final int targetSamplePostDec = targetSample / coarseDecimationRatio / fineDecimationRatio;
		private static final float dOverLambda = 0.5f;
		private static final float cfarThreshold = (float) (0.95 * dOverLambda * numChannels * (0.5 * pulseSize));
		public BeamFormer1Kernel() {
			Splitjoin<Byte, Float> splitJoin1 = new Splitjoin<>(
					new RoundrobinSplitter<Byte>(),
					new RoundrobinJoiner<Float>(2));
			for (int i = 0; i < numChannels; i++) {
				splitJoin1.add(new Pipeline<Float, Float>(
						new InputGenerate(i, numSamples, targetBeam, targetSample, cfarThreshold),
						new BeamFirFilter(numCoarseFilterTaps, numSamples, coarseDecimationRatio),
						new BeamFirFilter(numFineFilterTaps, numPostDec1, fineDecimationRatio)));
			}
			add(splitJoin1);

			Splitjoin<Float, Float> splitJoin2 = new Splitjoin<>(
					new DuplicateSplitter<Float>(),
					new RoundrobinJoiner<Float>());
			for (int i = 0; i < numBeams; i++) {
				splitJoin2.add(new Pipeline<Float, Float>(
						new BeamForm(i, numChannels),
						new BeamFirFilter(mfSize, numPostDec2, 1),
						new Magnitude()));
			}
			add(splitJoin2);
		}
	}

	private static final class InputGenerate extends StatefulFilter<Byte, Float> {
		private final int myChannel;
		private final int numberOfSamples;
		private final int tarBeam;
		private final int targetSample;
		private final float thresh;
		private int curSample;
		private final boolean holdsTarget;

		private InputGenerate(int myChannel, int numberOfSamples, int tarBeam,
				int targetSample, float thresh) {
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
			//dummy timing element
			pop();
			if (holdsTarget && (curSample == targetSample)) {
				push((float) Math.sqrt(curSample * myChannel));
				push((float) Math.sqrt(curSample * myChannel) + 1);
			} else {
				push((float) (-Math.sqrt(curSample * myChannel)));
				push((float) (-(Math.sqrt(curSample * myChannel) + 1)));
			}
			curSample++;
			if (curSample >= numberOfSamples)
				curSample = 0;
		}
	}

	private static final class BeamFirFilter extends StatefulFilter<Float, Float> {
		private final int numTaps;
		private final int inputLength;
		private final int decimationRatio;
		private final float[] real_weight;
		private final float[] imag_weight;
		private final int numTapsMinusOne;
		private final float[] realBuffer;
		private final float[] imagBuffer;
		private int count;
		private int pos;
		private BeamFirFilter(int numTaps, int inputLength, int decimationRatio) {
			super(2 * decimationRatio, 2, 0);
			this.numTaps = numTaps;
			this.inputLength = inputLength;
			this.decimationRatio = decimationRatio;
			this.real_weight = new float[numTaps];
			this.imag_weight = new float[numTaps];
			this.realBuffer = new float[numTaps];
			this.imagBuffer = new float[numTaps];
			this.numTapsMinusOne = numTaps - 1;
			this.pos = 0;
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

			realBuffer[numTapsMinusOne - pos] = pop();

			imagBuffer[numTapsMinusOne - pos] = pop();

			int modPos = numTapsMinusOne - pos;
			for (int i = 0; i < numTaps; i++) {
				real_curr += realBuffer[modPos] * real_weight[i]
						+ imagBuffer[modPos] * imag_weight[i];
				imag_curr += imagBuffer[modPos] * real_weight[i]
						+ realBuffer[modPos] * imag_weight[i];

				modPos = (modPos + 1) & numTapsMinusOne;
			}

			pos = (pos + 1) & numTapsMinusOne;

			push(real_curr);
			push(imag_curr);

			for (int i = 2; i < 2 * decimationRatio; i++) {
				pop();
			}

			count += decimationRatio;

			if (count == inputLength) {
				count = 0;
				pos = 0;
				for (int i = 0; i < numTaps; i++) {
					realBuffer[i] = 0;
					imagBuffer[i] = 0;
				}
			}
		}
	}

	private static final class CoarseBeamFirFilter extends Filter<Float, Float> {
		private final int numTaps;
		private final int inputLength;
		private final int decimationRatio;

		private final float[] real_weight;
		private final float[] imag_weight;

		private CoarseBeamFirFilter(int numTaps, int inputLength,
				int decimationRatio) {
			super(2 * inputLength, 2 * inputLength);
			this.numTaps = numTaps;
			this.inputLength = inputLength;
			this.decimationRatio = decimationRatio;
			real_weight = new float[numTaps];
			imag_weight = new float[numTaps];
			for (int j = 0; j < numTaps; j++) {
				int idx = j + 1;
				real_weight[j] = (float) (Math.sin(idx) / ((float) idx));
				imag_weight[j] = (float) (Math.cos(idx) / ((float) idx));
			}
		}

		@Override
		public void work() {
			int min = Math.min(numTaps, inputLength);
			for (int i = 1; i <= min; i++) {
				float real_curr = 0;
				float imag_curr = 0;
				for (int j = 0; j < i; j++) {
					int realIndex = 2 * (i - j - 1);
					int imagIndex = realIndex + 1;
					real_curr += real_weight[j] * peek(realIndex)
							+ imag_weight[j] * peek(imagIndex);
					imag_curr += real_weight[j] * peek(imagIndex)
							+ imag_weight[j] * peek(realIndex);
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
					real_curr += real_weight[j] * peek(realIndex)
							+ imag_weight[j] * peek(imagIndex);
					imag_curr += real_weight[j] * peek(imagIndex)
							+ imag_weight[j] * peek(realIndex);
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

	private static final class BeamForm extends Filter<Float, Float> {
		private final int myBeamId;
		private final int numChannels;
		private final float[] real_weight;
		private final float[] imag_weight;
		private BeamForm(int myBeamId, int numChannels) {
			super(2 * numChannels, 2);
			this.myBeamId = myBeamId;
			this.numChannels = numChannels;
			this.real_weight = new float[numChannels];
			this.imag_weight = new float[numChannels];
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

				real_curr += real_weight[i] * real_pop - imag_weight[i]
						* imag_pop;
				imag_curr += real_weight[i] * imag_pop + imag_weight[i]
						* real_pop;
			}
			push(real_curr);
			push(imag_curr);
		}
	}

	private final static class Magnitude extends Filter<Float, Float> {
		private Magnitude() {
			super(2, 1);
		}
		@Override
		public void work() {
			float f1 = pop();
			float f2 = pop();
			push((float)Math.sqrt(f1 * f1 + f2 * f2));
		}
	}

	private static class Detector extends Filter<Float, Float> {
		private final int _myBeam;
		private final int numSamples;
		private final int targetBeam;
		private final int targetSample;
		private float cfarThreshold;

		private int curSample;
		private int myBeam;
		private boolean holdsTarget;
		private float thresh;

		public Detector(int _myBeam, int numSamples, int targetBeam,
				int targetSample, float cfarThresholde) {
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
