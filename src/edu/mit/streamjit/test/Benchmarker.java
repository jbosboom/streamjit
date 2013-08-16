package edu.mit.streamjit.test;

import com.google.common.base.Stopwatch;
import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.test.Benchmark.Input;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.CheckVisitor;
import edu.mit.streamjit.impl.compiler.CompilerStreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/12/2013
 */
public final class Benchmarker {
	private static final long TIMEOUT = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);
	public static void main(String[] args) throws InterruptedException {
		StreamCompiler[] compilers = {
			new DebugStreamCompiler(),
			new CompilerStreamCompiler(),
			new CompilerStreamCompiler().multiplier(10),
			new CompilerStreamCompiler().multiplier(100),
			new CompilerStreamCompiler().multiplier(1000),
			new CompilerStreamCompiler().multiplier(10000),
		};
		ServiceLoader<Benchmark> loader = ServiceLoader.load(Benchmark.class);

		for (StreamCompiler sc : compilers) {
			for (Benchmark benchmark : loader) {
				benchmark.instantiate().visit(new CheckVisitor());
				for (Input input : benchmark.inputs())
					run(benchmark, input, sc);
			}
		}
	}

	private static void run(Benchmark benchmark, Input input, StreamCompiler compiler) {
		String statusText = null;
		long compileMillis = -1, runMillis = -1;
		Throwable throwable = null;
		try {
			Stopwatch stopwatch = new Stopwatch();
			stopwatch.start();
			CompiledStream<Object, Object> stream = compiler.compile(benchmark.instantiate());
			stopwatch.stop();
			compileMillis = stopwatch.elapsed(TimeUnit.MILLISECONDS);
			stopwatch.reset();

			Thread ot = new OutputThread(input.output(), stream);
			ot.start();
			Thread it = new InputThread(input.input(), stream);
			stopwatch.start();
			it.start();

			it.join(TIMEOUT);
			if (it.isAlive())
				throw new TimeoutException();
			ot.join(TIMEOUT);
			if (ot.isAlive())
				throw new TimeoutException();
			stopwatch.stop();
			runMillis = stopwatch.elapsed(TimeUnit.MILLISECONDS);
		} catch (TimeoutException ex) {
			statusText = "timed out";
		} catch (InterruptedException ex) {
			statusText = "interrupted";
		} catch (Throwable t) {
			statusText = "failed: "+t;
			throwable = t;
		}
		if (statusText == null)
			statusText = String.format("%d ms compile, %d ms run", compileMillis, runMillis);

		System.out.format("%s / %s / %s: %s%n", compiler, benchmark, input, statusText);
		if (throwable != null)
			throwable.printStackTrace(System.out);
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
