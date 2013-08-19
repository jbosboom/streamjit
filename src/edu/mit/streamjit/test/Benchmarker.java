package edu.mit.streamjit.test;

import com.google.common.base.Stopwatch;
import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.common.CheckVisitor;
import edu.mit.streamjit.impl.compiler.CompilerStreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;
import edu.mit.streamjit.test.Benchmark.Dataset;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/12/2013
 */
public final class Benchmarker {
	private static final long TIMEOUT_DURATION = 1;
	private static final TimeUnit TIMEOUT_UNIT = TimeUnit.MINUTES;
	public static void main(String[] args) throws InterruptedException {
		StreamCompiler[] compilers = {
			new DebugStreamCompiler(),
			new CompilerStreamCompiler(),
//			new CompilerStreamCompiler().multiplier(10),
//			new CompilerStreamCompiler().multiplier(100),
//			new CompilerStreamCompiler().multiplier(1000),
//			new CompilerStreamCompiler().multiplier(10000),
		};
		ServiceLoader<Benchmark> loader = ServiceLoader.load(Benchmark.class);

		for (StreamCompiler sc : compilers) {
			for (Benchmark benchmark : loader) {
				benchmark.instantiate().visit(new CheckVisitor());
				for (Dataset input : benchmark.inputs())
					run(benchmark, input, sc);
			}
		}
	}

	private static void run(Benchmark benchmark, Dataset input, StreamCompiler compiler) {
		String statusText = null;
		long compileMillis = -1, runMillis = -1;
		Throwable throwable = null;
		try {
			//TODO: make verify output
			Output<Object> output = Output.blackHole();
			Stopwatch stopwatch = new Stopwatch();
			stopwatch.start();
			CompiledStream stream = compiler.compile(benchmark.instantiate(), input.input(), output);
			compileMillis = stopwatch.elapsed(TimeUnit.MILLISECONDS);
			stream.awaitDrained(TIMEOUT_DURATION, TIMEOUT_UNIT);
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
}
