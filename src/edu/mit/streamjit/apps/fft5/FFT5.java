package edu.mit.streamjit.apps.fft5;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.concurrent.ConcurrentStreamCompiler;
import edu.mit.streamjit.impl.distributed.DistributedStreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;

/**
 * Rewritten StreamIt's asplos06 benchmarks. Refer
 * STREAMIT_HOME/apps/benchmarks/asplos06/fft/streamit/FFT5.str for original
 * implementations. Each StreamIt's language constructs (i.e., pipeline, filter
 * and splitjoin) are rewritten as classes in StreamJit.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Mar 14, 2013
 */
public class FFT5 {

	public static void main(String[] args) throws InterruptedException {
		FFT5Kernel kernel = new FFT5Kernel();
		// StreamCompiler sc = new DebugStreamCompiler();
		 StreamCompiler sc = new ConcurrentStreamCompiler(2);
		// StreamCompiler sc = new DistributedStreamCompiler(2);
		CompiledStream<Float, Void> stream = sc.compile(kernel);
		// Float output;
		for (float i = 0; i < 1000;) {
			if (stream.offer(i)) {
				// System.out.println("Offer success " + i);
				i++;
			} else {
				// System.out.println("Offer failed " + i);
				Thread.sleep(10);
			}
		}
		// Thread.sleep(1000);
		stream.drain();
		while (!stream.isDrained());
	}

	/**
	 * This represents "void->void pipeline FFT5()". FIXME: actual pipeline is
	 * void->void. Need to support void input, filereading, and file writing.
	 */
	public static class FFT5Kernel extends Pipeline<Float, Void> {
		public FFT5Kernel() {
			int N = 256;
			add(new FFTTestSource(N));
			// add FileReader<float>("../input/FFT5.in");
			add(new FFTReorder(N));
			for (int j = 2; j <= N; j *= 2) {
				add(new CombineDFT(j));
			}
			// add FileWriter<float>("FFT5.out");
			add(new FloatPrinter());
		}
	}

	private static class CombineDFT extends Filter<Float, Float> {

		float wn_r, wn_i;
		int n;

		CombineDFT(int n) {
			super(2 * n, 2 * n, 2 * n);
			this.n = n;
			init();
		}

		private void init() {
			wn_r = (float) Math.cos(2 * 3.141592654 / n);
			wn_i = (float) Math.sin(2 * 3.141592654 / n);
		}

		public void work() {
			int i;
			float w_r = 1;
			float w_i = 0;
			float[] results = new float[2 * n];

			for (i = 0; i < n; i += 2) {
				// this is a temporary work-around since there seems to be
				// a bug in field prop that does not propagate nWay into the
				// array references. --BFT 9/10/02

				// int tempN = nWay;
				// Fixed --jasperln

				// removed nWay, just using n --sitij 9/26/03

				float y0_r = peek(i);
				float y0_i = peek(i + 1);

				float y1_r = peek(n + i);
				float y1_i = peek(n + i + 1);

				float y1w_r = y1_r * w_r - y1_i * w_i;
				float y1w_i = y1_r * w_i + y1_i * w_r;

				results[i] = y0_r + y1w_r;
				results[i + 1] = y0_i + y1w_i;

				results[n + i] = y0_r - y1w_r;
				results[n + i + 1] = y0_i - y1w_i;

				float w_r_next = w_r * wn_r - w_i * wn_i;
				float w_i_next = w_r * wn_i + w_i * wn_r;
				w_r = w_r_next;
				w_i = w_i_next;
			}

			for (i = 0; i < 2 * n; i++) {
				pop();
				push(results[i]);
			}
		}

	}

	private static class FFTReorderSimple extends Filter<Float, Float> {

		int totalData;
		int n;

		FFTReorderSimple(int n) {
			super(2 * n, 2 * n, 2 * n);
			this.n = n;
			init();
		}

		private void init() {
			totalData = 2 * n;
		}

		public void work() {
			int i;

			for (i = 0; i < totalData; i += 4) {
				push(peek(i));
				push(peek(i + 1));
			}

			for (i = 2; i < totalData; i += 4) {
				push(peek(i));
				push(peek(i + 1));
			}

			for (i = 0; i < n; i++) {
				pop();
				pop();
			}
		}
	}

	private static class FFTReorder extends Pipeline<Float, Float> {
		FFTReorder(int n) {

			for (int i = 1; i < (n / 2); i *= 2)
				add(new FFTReorderSimple(n / i));
		}
	}

	private static class FFTTestSource extends Filter<Float, Float> {

		float max = 1000.0f;
		float current = 0.0f;
		int N;

		FFTTestSource(int N) {

			super(1, 2 * N);
			this.N = N;
		}

		public void work() {
			int i;
			// FIXME: Actual pop is 0. As StreamJit doesn't support void input,
			// it receives input and just pops out it.
			pop(); // As current implementation has no support to fire the
			// streamgraph with void element, we offer the graph with
			// random values and just pop out here.
			if (current > max)
				current = 0.0f;

			for (i = 0; i < 2 * (N); i++)
				push(current++);
		}
	}

	private static class FloatPrinter extends Filter<Float, Void> {

		FloatPrinter() {
			super(1, 0);
		}

		public void work() {
			System.out.println(pop());
		}
	}
}
