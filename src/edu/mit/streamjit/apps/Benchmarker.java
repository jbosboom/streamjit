package edu.mit.streamjit.apps;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.apps.fmradio.FMRadio;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.BlobHostStreamCompiler;
import edu.mit.streamjit.impl.common.CheckVisitor;
import edu.mit.streamjit.impl.compiler.CompilerBlobFactory;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/12/2013
 */
public final class Benchmarker {
	public static void main(String[] args) throws InterruptedException {
		StreamCompiler sc = new BlobHostStreamCompiler(new CompilerBlobFactory(), 1);
		Benchmark benchmark = new FMRadio.FMRadioBenchmark();

		benchmark.instantiate().visit(new CheckVisitor());

		Benchmark.Input input = benchmark.inputs().get(0);
		CompiledStream<Object, Object> stream = sc.compile(benchmark.instantiate());
		Thread ot = new OutputThread(input.output(), stream);
		ot.start();
		Thread it = new InputThread(input.input(), stream);
		it.start();

		it.join();
		ot.join();
	}

	private static class InputThread extends Thread {
		private final Buffer input;
		private final CompiledStream<Object, Object> stream;
		private InputThread(Buffer input, CompiledStream<Object, Object> stream) {
			super("benchmark-input");
			this.input = input;
			this.stream = stream;
		}
		@Override
		@SuppressWarnings("empty-statement")
		public void run() {
			Object o;
			while ((o = input.read()) != null) {
				while (!stream.offer(o))
					/* intentional empty statement */;
			}
			stream.drain();
		}
	}

	private static class OutputThread extends Thread {
		private final Buffer output;
		private final CompiledStream<Object, Object> stream;
		private OutputThread(Buffer output, CompiledStream<Object, Object> stream) {
			super("benchmark-output");
			this.output = output;
			this.stream = stream;
		}
		@Override
		public void run() {
			Object o;
			while ((o = stream.poll()) != null || !stream.isDrained()) {
				if (output != null && o != null)
					if (!o.equals(output.read()))
						throw new AssertionError("bad output");
			}
		}
	}
}
