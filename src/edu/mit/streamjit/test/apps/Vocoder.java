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
package edu.mit.streamjit.test.apps;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.UnmodifiableIterator;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.DuplicateSplitter;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StatefulFilter;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.WeightedRoundrobinJoiner;
import edu.mit.streamjit.api.WeightedRoundrobinSplitter;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmarker;
import edu.mit.streamjit.test.Datasets;
import edu.mit.streamjit.test.SuppliedBenchmark;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Iterator;

/**
 * Ported from streams/apps/benchmarks/asplos06/vocoder/streamit/VocoderTopLevel.str
 *
 * TODO: to verify this against classic StreamIt, implement prework in
 * FirstDifference.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 2/11/2014
 */
public final class Vocoder {
	private Vocoder() {}

	public static void main(String[] args) throws InterruptedException {
		StreamCompiler sc = new DebugStreamCompiler();
		Benchmarker.runBenchmark(new VocoderBenchmark(), sc).get(0).print(System.out);
	}

	@ServiceProvider(Benchmark.class)
	public static final class VocoderBenchmark extends SuppliedBenchmark {
		private static final int COPIES = 1;
		private static final Iterable<Integer> INPUT = ImmutableList.copyOf(Iterables.concat(
				//simulating a Delay filter's prework
				Collections.nCopies(VocoderTopLevel.DFT_LENGTH_NOM, 0),
				//limit based on VocoderExample.in, not relevant to
				//VocoderTopLevel.str, which takes its input from a StepSource
				//source filter
				Iterables.limit(new StepSource(100), 10000)
		));
		public VocoderBenchmark() {
			super("Vocoder", VocoderTopLevel.class, new Dataset("StepSource", (Input)Datasets.nCopies(COPIES, (Input)Input.fromIterable(INPUT))
					//classic StreamIt runs forever here, so length will vary.
//					, (Supplier)Suppliers.ofInstance((Input)Input.fromBinaryFile(Paths.get("/home/jbosboom/streamit/streams/apps/benchmarks/asplos06/vocoder/streamit/VocoderTopLevel.out"), Integer.class, ByteOrder.LITTLE_ENDIAN))
			));
		}
	}

	private static final class VocoderTopLevel extends Pipeline<Float, Float> {
		private static final int DFT_LENGTH_NOM = 28;
		private static final int DFT_LENGTH = DFT_LENGTH_NOM/2 + 1;
		private static final float FREQUENCY_FACTOR = 0.6f;
		private static final float GLOTTAL_EXPANSION = 1f/1.2f;
		private static final int NEW_LENGTH = (int)(DFT_LENGTH * GLOTTAL_EXPANSION / FREQUENCY_FACTOR);
		private static final int DFT_LENGTH_REDUCED = 3;
		private static final int NEW_LENGTH_REDUCED = 4;
		private static final float SPEED_FACTOR = 1.0f;
		private static final int n_LENGTH = 1;
		private static final int m_LENGTH = 1;
		public VocoderTopLevel() {
			super(new IntToFloat(),
					//TODO prework; for now prepended to input
					//new Delay(DFT_LENGTH_NOM),
					new FilterBank(DFT_LENGTH_NOM),
					new RectangularToPolar(),
					new Splitjoin<Float, Float>(new RoundrobinSplitter<Float>(), new RoundrobinJoiner<Float>(),
							new MagnitudeStuff(DFT_LENGTH_REDUCED, NEW_LENGTH_REDUCED, m_LENGTH, n_LENGTH, DFT_LENGTH, NEW_LENGTH, SPEED_FACTOR),
							new PhaseStuff(n_LENGTH, m_LENGTH, DFT_LENGTH_REDUCED, NEW_LENGTH_REDUCED, DFT_LENGTH, NEW_LENGTH, FREQUENCY_FACTOR, SPEED_FACTOR)
					),
					new PolarToRectangular(),
					new SumReals(NEW_LENGTH),
					new InvDelay((DFT_LENGTH - 2) * m_LENGTH / n_LENGTH),
					new FloatToShort()
			);
		}
	}

	private static final class IntToFloat extends Filter<Integer, Float> {
		private IntToFloat() {
			super(1, 1);
		}
		@Override
		public void work() {
			push(pop().floatValue());
		}
	}

	private static final class FilterBank extends Splitjoin<Float, Float> {
		private FilterBank(int channels) {
			super(new DuplicateSplitter<Float>(), new RoundrobinJoiner<Float>(2));
			for (int k = 0; k <= channels/2; ++k)
				add(new DFTFilter(channels, (float)(2*Math.PI*k/channels)));
		}
	}

	private static final class DFTFilter extends StatefulFilter<Float, Float> {
		private final int DFTLen;
		//the rate by which to deteriorate, assuring stability
		private final float deter;
		//since the previous complex value is multiplied by the deter each
		//time, by the time the last time sample is windowed out it's
		//effect will have been multiplied by deter DFTLen times, hence it
		//needs to be multiplied by deter^DFTLen before being subtracted
		private final float detern;
		private final float wR, wI; //represents w^(-k)
		private float prevR, prevI;
		private float nextR, nextI;
		private DFTFilter(int DFTLen, float range) {
			super(1, 2, DFTLen+1);
			this.DFTLen = DFTLen;
			deter = 0.999999f;
			detern = 1.0f;
			wR = (float)Math.cos(range);
			wI = (float)-Math.sin(range);
			prevR = 0;
			prevI = 0;
		}
		@Override
		public void work() {
			float nextVal = peek(DFTLen);
			float current = pop();

			prevR = prevR * deter + (nextVal - (detern * current));
			prevI = prevI * deter;
			nextR = prevR * wR - prevI * wI;
			nextI = prevR * wI + prevI * wR;
			prevR = nextR;
			prevI = nextI;

			push(prevR);
			push(prevI);
		}
	}

	private static final class RectangularToPolar extends Filter<Float, Float> {
		private RectangularToPolar() {
			super(2, 2);
		}
		@Override
		public void work() {
			float x = pop();
			float y = pop();
			float r = (float)Math.sqrt(x * x + y * y);
			float theta = (float)Math.atan2(y, x);
			push(r);
			push(theta);
		}
	}

	private static final class MagnitudeStuff extends Pipeline<Float, Float> {
		private MagnitudeStuff(int DFTLen_red, int newLen_red, int n_len, int m_len, int DFTLen, int newLen, float speed) {
			super();
			if (DFTLen != newLen) {
				add(new Splitjoin<Float, Float>(new DuplicateSplitter<Float>(), new RoundrobinJoiner<Float>(),
						new FIRSmoothingFilter(DFTLen),
						new Identity<Float>()));
				add(new Deconvolve());
				add(new Splitjoin<Float, Float>(new RoundrobinSplitter<Float>(), new RoundrobinJoiner<Float>(),
						new Duplicator(DFTLen_red, newLen_red),
						Remapper(DFTLen_red, newLen_red)));
				add(new Multiplier());
			}
			if (speed != 1.0) {
				Splitjoin<Float, Float> sj = new Splitjoin<>(new RoundrobinSplitter<Float>(), new RoundrobinJoiner<Float>());
				for(int i=0; i<DFTLen; i++)
					sj.add(Remapper(n_len, m_len));
				add(sj);
			} else
				add(new Identity<Float>());
		}
	}

	private static final class FIRSmoothingFilter extends Filter<Float, Float> {
		private static final float[] cosWin = {0.1951f, 0.3827f, 0.5556f, 0.7071f, 0.8315f, 0.9239f, 0.9808f, 1.0000f, 0.9808f, 0.9239f, 0.8315f, 0.7071f, 0.5556f, 0.3827f, 0.1951f};
		private static final int offset = cosWin.length / 2;
		private final int DFTLen;
		private FIRSmoothingFilter(int DFTLen) {
			super(DFTLen, DFTLen, cosWin.length);
			this.DFTLen = DFTLen;
		}
		@Override
		public void work() {
			//note that h[k] = h[i + off]
			for (int n = 0; n < DFTLen; n++) {
				float y = 0;
				for (int k = 0; k < cosWin.length; k++) {
					int i = k - offset; //so that when i = 0, k will be at the center
					if (((n - i) >= 0) && ((n - i) < DFTLen))
						y += peek(n - i) * cosWin[k];
				}
				push(y);
			}

			for (int i = 0; i < DFTLen; i++)
				pop();
		}
	}

	private static final class Deconvolve extends Filter<Float, Float> {
		private Deconvolve() {
			super(2, 2);
		}
		@Override
		public void work() {
			float den = pop();
			float num = pop();
			push(den);
			if (den == 0.0)
				push(0.0f);
			else
				push(num / den);
		}
	}

	private static final class Duplicator extends Filter<Float, Float> {
		private final int oldLen, newLen;
		private Duplicator(int oldLen, int newLen) {
			super(oldLen, newLen);
			this.oldLen = oldLen;
			this.newLen = newLen;
		}
		@Override
		public void work() {
			if (newLen <= oldLen) {
				for (int i = 0; i < newLen; i++)
					push(pop());
				for (int i = newLen; i < oldLen; i++)
					pop();
			} else {
				//TODO: use peeking instead.
				float[] orig = new float[oldLen];
				for (int i = 0; i < oldLen; i++)
					orig[i] = pop();
				for (int i = 0; i < newLen; i++)
					push(orig[i % oldLen]);
			}
		}
	}

	private static OneToOneElement<Float, Float> Remapper(int oldLen, int newLen) {
		if (oldLen == newLen)
			return new Identity<Float>();
		Pipeline<Float, Float> p = new Pipeline<>();
		if (newLen != 1)
			p.add(new LinearInterpolator(newLen));
		if (oldLen != 1)
			p.add(new Decimator(oldLen));
		return p;
	}

	private static final class LinearInterpolator extends Filter<Float, Float> {
		private final int interp;
		private LinearInterpolator(int interp) {
			super(1, interp, 2);
			this.interp = interp;
		}
		@Override
		public void work() {
			float base = pop();
			float diff = peek(0) - base;
			float interp_f = (float)interp;
			float i_f;

			push(base);
			//already pushed 1, so just push another (interp - 1) floats
			for (int i = 1; i < interp; i++) {
				i_f = (float)i;
				push(base + (i_f / interp_f) * diff);
			}
		}
	}

	private static final class Decimator extends Filter<Float, Float> {
		private final int decim;
		private Decimator(int decim) {
			super(decim, 1);
			this.decim = decim;
		}
		@Override
		public void work() {
			push(pop());
			//for(int goal=decim-1; goal>0; goal--)
			//    pop();
			for (int goal = 0; goal < decim - 1; goal++)
				pop();
		}
	}

	private static final class Multiplier extends Filter<Float, Float> {
		private Multiplier() {
			super(2, 1);
		}
		@Override
		public void work() {
			push(pop()*pop());
		}
	}

	private static final class PhaseStuff extends Pipeline<Float, Float> {
		private PhaseStuff(int n_len, int m_len, int DFTLen_red, int newLen_red, int DFTLen, int newLen, float c, float speed) {
			super();
			if (speed != 1.0 || c != 1.0) {
				Splitjoin<Float, Float> sj = new Splitjoin<>(new RoundrobinSplitter<Float>(), new RoundrobinJoiner<Float>());
				for (int i = 0; i < DFTLen; ++i)
					sj.add(new InnerPhaseStuff(n_len, m_len, c, speed));
				add(sj);
			}
			if (newLen != DFTLen)
				add(new Duplicator(DFTLen_red, newLen_red));
			else
				add(new Identity<Float>());
		}
	}

	private static final class InnerPhaseStuff extends Pipeline<Float, Float> {
		private InnerPhaseStuff(int n_len, int m_len, float c, float speed) {
			super();
			add(new PhaseUnwrapper());
			add(new FirstDifference());
			if (c != 1.0)
				add(new ConstMultiplier(c));
			if (speed != 1.0)
				add(Remapper(n_len, m_len));
			add(new Accumulator());
		}
	}

	/**
	 * Porting note: unused 'estimate' field removed.
	 */
	private static final class PhaseUnwrapper extends StatefulFilter<Float, Float> {
		private static final float pi = (float)Math.PI;
		private float previous = 0;
		private PhaseUnwrapper() {
			super(1, 1);
		}
		@Override
		public void work() {
			float unwrapped = pop();
			float delta = unwrapped - previous;

			while (delta > 2 * pi * (11.0 / 16.0)) {
				unwrapped -= 2 * pi;
				delta -= 2 * pi;
			}
			while (delta < -2 * pi * (11.0 / 16.0)) {
				unwrapped += 2 * pi;
				delta += 2 * pi;
			}
			previous = unwrapped;
			push(unwrapped);
		}
	}

	private static final class FirstDifference extends Filter<Float, Float> {
		private FirstDifference() {
			super(1, 1, 2);
		}
//		//TODO
//		public void prework() {
//			push(peek(0));
//		}
		@Override
		public void work() {
			push(peek(1) - peek(0));
			pop();
		}
	}

	private static final class ConstMultiplier extends Filter<Float, Float> {
		private final float mult;
		private ConstMultiplier(float mult) {
			super(1, 1);
			this.mult = mult;
		}
		@Override
		public void work() {
			push(pop() * mult);
		}
	}

	private static final class Accumulator extends StatefulFilter<Float, Float> {
		private float val = 0;
		private Accumulator() {
			super(1, 1);
		}
		@Override
		public void work() {
			val += pop();
			push(val);
		}
	}

	private static final class PolarToRectangular extends Filter<Float, Float> {
		private PolarToRectangular() {
			super(2, 2);
		}
		@Override
		public void work() {
			float r = pop();
			float theta = pop();
			push(r * (float)Math.cos(theta));
			push(r * (float)Math.sin(theta));
		}
	}

	private static final class SumReals extends Splitjoin<Float, Float> {
		private SumReals(int DFTLen) {
			super(new RoundrobinSplitter<Float>(), new WeightedRoundrobinJoiner<Float>(1, 0),
					new SumRealsRealHandler(DFTLen),
					new FloatVoid());
		}
	}

	private static final class SumRealsRealHandler extends Pipeline<Float, Float> {
		private SumRealsRealHandler(int DFTLen) {
			super();
			add(new Splitjoin<Float, Float>(new WeightedRoundrobinSplitter<Float>(1, DFTLen-2, 1), new WeightedRoundrobinJoiner<Float>(1, DFTLen-2, 1),
					new Identity<Float>(),
					new Doubler(),
					new Identity<Float>()));
			if ((DFTLen % 2) != 0)
				add(new Padder(DFTLen, 0, 1));
			add(new Splitjoin<Float, Float>(new RoundrobinSplitter<Float>(), new RoundrobinJoiner<Float>(),
					new Adder((DFTLen+1)/2),
					new Adder((DFTLen+1)/2)));
			add(new Subtractor());
			add(new ConstMultiplier((float)(1.0/((DFTLen-1)*2))));
		}
	}

	private static final class Doubler extends Filter<Float, Float> {
		private Doubler() {
			super(1, 1);
		}
		@Override
		public void work() {
			float x = pop();
			push(x+x);
		}
	}

	private static final class Padder extends Filter<Float, Float> {
		private final int length, front, back;
		private Padder(int length, int front, int back) {
			super(length+front+back, length);
			this.length = length;
			this.front = front;
			this.back = back;
		}
		@Override
		public void work() {
			for(int i = 0; i < front; i++)
				push(0.0f);

			for (int i = 0; i < length; i++)
				push(pop());

			for (int i = 0; i < back; i++)
				push(0.0f);
		}
	}

	private static final class Adder extends Filter<Float, Float> {
		private final int length;
		private Adder(int length) {
			super(length, 1);
			this.length = length;
		}
		@Override
		public void work() {
			float val = 0;
			for (int i = 0; i < length; i++)
				val += pop();
			push(val);
		}
	}

	private static final class Subtractor extends Filter<Float, Float> {
		private Subtractor() {
			super(2, 1);
		}
		@Override
		public void work() {
			push(pop() - pop());
		}
	}

	private static final class FloatVoid extends Filter<Float, Float> {
		private FloatVoid() {
			super(1, 0);
		}
		@Override
		public void work() {
			pop();
		}
	}

	private static final class InvDelay extends Filter<Float, Float> {
		//TODO: prework that throws away the delay elements?
		private final int n;
		private InvDelay(int N) {
			super(1, 1, N+1);
			this.n = N;
		}
		@Override
		public void work() {
			push(peek(n));
			pop();
		}
	}

	private static final class FloatToShort extends Filter<Float, Integer> {
		private FloatToShort() {
			super(1, 1);
		}
		@Override
		public void work() {
			int s;
			float fs = pop() + 0.5f;
			fs = (fs > 32767.0f ? 32767.0f : (fs < -32767.0f ? -32767.0f : fs));
			s = (int)fs;
			push(s);
		}
	}

	private static final class StepSource implements Iterable<Integer> {
		private final int length;
		private StepSource(int length) {
			this.length = length;
		}
		@Override
		public Iterator<Integer> iterator() {
			return new UnmodifiableIterator<Integer>() {
				private int x = 1;
				private boolean up = true;
				@Override
				public boolean hasNext() {
					return true;
				}
				@Override
				public Integer next() {
					if (x == length)
						up = false;
					else if (x == 0)
						up = true;
					if (up)
						return x++;
					else
						return x--;
				}
			};
		}
	}
}
