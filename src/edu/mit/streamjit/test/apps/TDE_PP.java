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
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmark.Dataset;
import edu.mit.streamjit.test.Benchmarker;
import edu.mit.streamjit.test.Datasets;
import edu.mit.streamjit.test.SuppliedBenchmark;
import java.nio.ByteOrder;
import java.nio.file.Paths;

/**
 * Ported from streamit/streams/apps/benchmarks/asplos06/tde_pp/streamit/tde_pp.str
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 2/11/2014
 */
public final class TDE_PP {
	private TDE_PP() {}

	public static void main(String[] args) {
		StreamCompiler sc = new DebugStreamCompiler();
		Benchmarker.runBenchmark(new TDE_PPBenchmark(), sc).get(0).print(System.out);
	}

	@ServiceProvider(Benchmark.class)
	public static final class TDE_PPBenchmark extends SuppliedBenchmark {
		private static final int COPIES = 1;
		public TDE_PPBenchmark() {
			super("TDE_PP", TDE_PPKernel.class, new Dataset("tde_pp.in",
					(Input)Datasets.nCopies(COPIES, (Input)Input.fromBinaryFile(Paths.get("data/tde_pp.in"), Float.class, ByteOrder.LITTLE_ENDIAN))
//					, (Supplier)Suppliers.ofInstance((Input)Input.fromBinaryFile(Paths.get("/home/jbosboom/streamit/streams/apps/benchmarks/asplos06/tde_pp/streamit/tde_pp.out"), Float.class, ByteOrder.LITTLE_ENDIAN))
			));
		}
	}

	public static final class TDE_PPKernel extends Pipeline<Float, Float> {
		/* The number of channels or sensors receiving data. */
		private static final int CH = 6; // N_CH in C Code.
		/* The number of range (distance from antenna) gates. */
		private static final int N = 36; // number of input samples  (N_RG in C Code)
		/* Pulse repetition intervals per data cube */
		private static final int M = 15; // number of pri's in a  (N_PRI in C Code)
		private static final int B = 64; // smallest power of 2 above N
					// Data is [N_CH][N_RG][N_PRI][2]
		private static final float mult = 0.00390625f; // multiplier
		private static final int DataParallelism = 1; // any number 0 < DataParallelism <= N
		private static final int FFTDataParallelism = 1; // any number < number of DFTs calculated
		public TDE_PPKernel() {
			super();
			add(new Transpose(N, M));
			if (DataParallelism == 1)
				// Redundant, for versions of the compiler that do not
				// optimize away a splitjoint that does nothing.
				add(new Pipeline(
					new Expand(N, B),	                   // up to power of 2 for fft
					new FFTKernel4(B, FFTDataParallelism), // fft
					new Multiply_by_float(B, mult),        // mult
					new IFFTKernel4(B, FFTDataParallelism),// ifft
					new Contract(N, B)                     // back to original size
				));
//			else {
//				// Same as above with data-parallelism.
//				Splitjoin<Float, Float> sj = new Splitjoin<>(new RoundrobinSplitter<Float>(2), new RoundrobinJoiner<Float>(2));
//				for (int i = 0; i < DataParallelism; ++i)
//					sj.add(new Pipeline(
//						new Expand(N, B),	                   // up to power of 2 for fft
//						new FFTKernel4(B, FFTDataParallelism), // fft
//						new Multiply_by_float(B, mult),        // mult
//						new IFFTKernel4(B, FFTDataParallelism),// ifft
//						new Contract(N, B)                     // back to original size
//					));
//			}
			add(new Transpose(M, N));
		}
	}

	private static final class Transpose extends Filter<Float, Float> {
		private final int M, N;
		private Transpose(int M, int N) {
			super(M*N*2, M*N*2, M*N*2);
			this.M = M;
			this.N = N;
		}
		@Override
		public void work() {
			for(int i=0; i<M; i++) {
				for(int j=0; j<N; j++) {
					push(peek(i*N*2+j*2));
					push(peek(i*N*2+j*2+1));
				}
			}
			for(int i=0; i<M; i++) {
				for(int j=0; j<N; j++) {
					pop();
					pop();
				}
			}
		}
	}

	// expand data for next-largest power of 2 (known to be 64)
	private static final class Expand extends Filter<Float, Float> {
		private final int N, B;
		private Expand(int N, int B) {
			super(2*N, 2*B);
			this.N = N;
			this.B = B;
		}
		@Override
		public void work() {
			for (int i = 0; i < 2 * N; i++)
				push(pop());
			for (int i = 2 * N; i < 2 * B; i++)
				push(0.0f);
		}
	}

	private static final class FFTKernel4 extends Pipeline<Float, Float> {
		private FFTKernel4(int n, int DataParallelism) {
			super();
			if (DataParallelism == 1) {
				add(new FFTReorder(n));
				for (int j = 2; j <= n; j *= 2)
					add(new CombineDFT(j));
			} else {
				Splitjoin<Float, Float> sj = new Splitjoin<>(new RoundrobinSplitter<Float>(2*n), new RoundrobinJoiner<Float>(2*n));
				for (int i = 0; i < DataParallelism; ++i) {
					Pipeline<Float, Float> p = new Pipeline();
					p.add(new FFTReorder(n));
					for (int j = 2; j <= n; j *= 2)
						p.add(new CombineDFT(j));
					sj.add(p);
				}
				add(sj);
			}
		}
	}

	private static final class IFFTKernel4 extends Pipeline<Float, Float> {
		private IFFTKernel4(int n, int DataParallelism) {
			super();
			if (DataParallelism == 1) {
				add(new FFTReorder(n));
				for (int j = 2; j < n; j *= 2)
					add(new CombineIDFT(j));
				add(new CombineIDFTFinal(n));
			} else {
				Splitjoin<Float, Float> sj = new Splitjoin<>(new RoundrobinSplitter<Float>(2*n), new RoundrobinJoiner<Float>(2*n));
				for (int i = 0; i < DataParallelism; ++i) {
					Pipeline<Float, Float> p = new Pipeline();
					p.add(new FFTReorder(n));
					for (int j = 2; j < n; j *= 2)
						p.add(new CombineIDFT(j));
					p.add(new CombineIDFTFinal(n));
					sj.add(p);
				}
				add(sj);
			}
		}
	}

	private static final class FFTReorder extends Pipeline<Float, Float> {
		private FFTReorder(int n) {
			super();
			for (int i = 1; i < (n/2); i*= 2)
				add(new FFTReorderSimple(n/i));
		}
	}

	private static final class FFTReorderSimple extends Filter<Float, Float> {
		private final int totalData;
		private FFTReorderSimple(int n) {
			super(2*n, 2*n, 2*n);
			this.totalData = 2*n;
		}
		@Override
		public void work() {
			for (int i = 0; i < totalData; i += 4) {
				push(peek(i));
				push(peek(i + 1));
			}
			for (int i = 2; i < totalData; i += 4) {
				push(peek(i));
				push(peek(i + 1));
			}
			for (int i = 0; i < totalData; i++)
				pop();
		}
	}

	private static final class CombineDFT extends Filter<Float, Float> {
		private final int n;
		private final float[] w;
		private CombineDFT(int n) {
			super(2*n, 2*n, 2*n);
			this.n = n;
			this.w = new float[n];
			float wn_r = (float)Math.cos(2 * 3.141592654 / n);
			float wn_i = (float)Math.sin(-2 * 3.141592654 / n);
			float real = 1;
			float imag = 0;
			float next_real, next_imag;
			for (int i = 0; i < n; i += 2) {
				w[i] = real;
				w[i + 1] = imag;
				next_real = real * wn_r - imag * wn_i;
				next_imag = real * wn_i + imag * wn_r;
				real = next_real;
				imag = next_imag;
			}
		}
		@Override
		public void work() {
			float[] results = new float[2 * n];
			for (int i = 0; i < n; i += 2) {
				int i_plus_1 = i + 1;

				float y0_r = peek(i);
				float y0_i = peek(i_plus_1);

				float y1_r = peek(n + i);
				float y1_i = peek(n + i_plus_1);

				// load into temps to make sure it doesn't got loaded
				// separately for each load
				float weight_real = w[i];
				float weight_imag = w[i_plus_1];

				float y1w_r = y1_r * weight_real - y1_i * weight_imag;
				float y1w_i = y1_r * weight_imag + y1_i * weight_real;

				results[i] = y0_r + y1w_r;
				results[i + 1] = y0_i + y1w_i;

				results[n + i] = y0_r - y1w_r;
				results[n + i + 1] = y0_i - y1w_i;
			}

			for (int i = 0; i < 2 * n; i++) {
				pop();
				push(results[i]);
			}
		}
	}

	private static final class CombineIDFT extends Filter<Float, Float> {
		private final int n;
		private final float[] w;
		private CombineIDFT(int n) {
			super(2*n, 2*n, 2*n);
			this.n = n;
			this.w = new float[n];
			float wn_r = (float)Math.cos(2 * 3.141592654 / n);
			float wn_i = (float)Math.sin(2 * 3.141592654 / n);
			float real = 1;
			float imag = 0;
			float next_real, next_imag;
			for (int i = 0; i < n; i += 2) {
				w[i] = real;
				w[i + 1] = imag;
				next_real = real * wn_r - imag * wn_i;
				next_imag = real * wn_i + imag * wn_r;
				real = next_real;
				imag = next_imag;
			}
		}
		@Override
		public void work() {
			float[] results = new float[2 * n];

			for (int i = 0; i < n; i += 2) {
				int i_plus_1 = i + 1;

				float y0_r = peek(i);
				float y0_i = peek(i_plus_1);

				float y1_r = peek(n + i);
				float y1_i = peek(n + i_plus_1);

				// load into temps to make sure it doesn't got loaded
				// separately for each load
				float weight_real = w[i];
				float weight_imag = w[i_plus_1];

				float y1w_r = y1_r * weight_real - y1_i * weight_imag;
				float y1w_i = y1_r * weight_imag + y1_i * weight_real;

				results[i] = y0_r + y1w_r;
				results[i + 1] = y0_i + y1w_i;

				results[n + i] = y0_r - y1w_r;
				results[n + i + 1] = y0_i - y1w_i;
			}

			for (int i = 0; i < n; i++) {
				pop();
				pop();
			}

			for (int i = 0; i < 2 * n; i += 2) {
				push(results[i]);
				push(results[i + 1]);
			}
		}
	}

	private static final class CombineIDFTFinal extends Filter<Float, Float> {
		private final int n;
		private final float[] w;
		private final float n_recip;
		private CombineIDFTFinal(int n) {
			super(2*n, 2*n, 2*n);
			this.n = n;
			this.w = new float[n];
			float wn_r = (float)Math.cos(2 * 3.141592654 / n);
			float wn_i = (float)Math.sin(2 * 3.141592654 / n);
			n_recip = 1.0f / ((float)n);
			// scales coefficients for y1 (but not y0)
			float real = n_recip;
			float imag = 0;
			float next_real, next_imag;
			for (int i = 0; i < n; i += 2) {
				w[i] = real;
				w[i + 1] = imag;
				next_real = real * wn_r - imag * wn_i;
				next_imag = real * wn_i + imag * wn_r;
				real = next_real;
				imag = next_imag;
			}
		}
		@Override
		public void work() {
			float[] results = new float[2 * n];

			for (int i = 0; i < n; i += 2) {
				int i_plus_1 = i + 1;

				// y0: extra mult to scale
				float y0_r = n_recip * peek(i);
				float y0_i = n_recip * peek(i_plus_1);

				float y1_r = peek(n + i);
				float y1_i = peek(n + i_plus_1);

				// load into temps to make sure it doesn't got loaded
				// separately for each load
				float weight_real = w[i];
				float weight_imag = w[i_plus_1];

				float y1w_r = y1_r * weight_real - y1_i * weight_imag;
				float y1w_i = y1_r * weight_imag + y1_i * weight_real;

				results[i] = y0_r + y1w_r;
				results[i + 1] = y0_i + y1w_i;

				results[n + i] = y0_r - y1w_r;
				results[n + i + 1] = y0_i - y1w_i;
			}

			for (int i = 0; i < n; i++) {
				pop();
				pop();
			}

			for (int i = 0; i < 2 * n; i += 2) {
				push(results[i]);
				push(results[i + 1]);
			}
		}
	}

	private static final class Multiply_by_float extends Filter<Float, Float> {
		private final int B;
		private final float m;
		private Multiply_by_float(int B, float m) {
			super(2*B, 2*B);
			this.B = B;
			this.m = m;
		}
		@Override
		public void work() {
			for (int j = 0; j < B; j++) {
				push(pop() * m);
				push(pop() * m);
			}
		}
	}

	private static final class Contract extends Filter<Float, Float> {
		private final int N, B;
		private Contract(int N, int B) {
			super(2*B, 2*N);
			this.N = N;
			this.B = B;
		}
		@Override
		public void work() {
			for (int i = 0; i < 2 * N; i++)
				push(pop());
			for (int i = 2 * N; i < 2 * B; i++)
				pop();
		}
	}
}
