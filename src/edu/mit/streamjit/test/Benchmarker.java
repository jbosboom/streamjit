package edu.mit.streamjit.test;

import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Rate;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.StatefulFilter;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.StreamVisitor;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.AbstractWriteOnlyBuffer;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.CheckVisitor;
import edu.mit.streamjit.impl.common.InputBufferFactory;
import edu.mit.streamjit.impl.common.OutputBufferFactory;
import edu.mit.streamjit.impl.compiler.CompilerStreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;
import edu.mit.streamjit.test.Benchmark.Dataset;
import edu.mit.streamjit.util.CountingExecutorCompletionService;
import edu.mit.streamjit.util.ReflectionUtils;
import edu.mit.streamjit.util.SkipMissingServicesIterator;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * A test harness for Benchmark instances.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/12/2013
 */
public final class Benchmarker {
	public static enum Attribute {
		APP, SANITY, REGRESSION,
		STATIC, DYNAMIC, PEEKING, STATELESS, STATEFUL
	}

	public static final ImmutableMap<String, Attribute> PACKAGE_TO_ATTRIBUTE = ImmutableMap.of(
			"edu.mit.streamjit.test.apps", Attribute.APP,
			"edu.mit.streamjit.test.sanity", Attribute.SANITY,
			"edu.mit.streamjit.test.regression", Attribute.REGRESSION
			);

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		OptionParser parser = new OptionParser();
//		ArgumentAcceptingOptionSpec<String> requiredTestClasses = parser.accepts("require-test-class")
//				.withRequiredArg().withValuesSeparatedBy(',').ofType(String.class);
		ArgumentAcceptingOptionSpec<String> includedStreamClasses = parser.accepts("include-stream-class")
				.withRequiredArg().withValuesSeparatedBy(',').ofType(String.class);
		ArgumentAcceptingOptionSpec<String> excludedStreamClasses = parser.accepts("exclude-stream-class")
				.withRequiredArg().withValuesSeparatedBy(',').ofType(String.class);
		ArgumentAcceptingOptionSpec<Attribute> includedAttributes = parser.accepts("include-attribute")
				.withRequiredArg().withValuesSeparatedBy(',').ofType(Attribute.class);
		ArgumentAcceptingOptionSpec<Attribute> excludedAttributes = parser.accepts("exclude-attribute")
				.withRequiredArg().withValuesSeparatedBy(',').ofType(Attribute.class);
		parser.accepts("check");

		OptionSet options = parser.parse(args);
		ImmutableSet<String> includedClasses = ImmutableSet.copyOf(includedStreamClasses.values(options));
		ImmutableSet<String> excludedClasses = ImmutableSet.copyOf(excludedStreamClasses.values(options));
		EnumSet<Attribute> includedAttrs = options.has(includedAttributes) ? EnumSet.copyOf(includedAttributes.values(options)) : EnumSet.noneOf(Attribute.class);
		EnumSet<Attribute> excludedAttrs = options.has(excludedAttributes) ? EnumSet.copyOf(excludedAttributes.values(options)) : EnumSet.noneOf(Attribute.class);

		ExecutorService executor = Executors.newFixedThreadPool(8);
		CountingExecutorCompletionService<Result> completionService = new CountingExecutorCompletionService<>(executor);
		for (Iterator<BenchmarkProvider> providerIterator = new SkipMissingServicesIterator<>(ServiceLoader.load(BenchmarkProvider.class).iterator()); providerIterator.hasNext();)
			completionService.submit(new BenchmarkProviderFilterTask(providerIterator.next(),
					includedClasses, excludedClasses, includedAttrs, excludedAttrs, completionService), null);

		while (completionService.pendingTasks() > 0) {
			Result r = completionService.take().get();
			if (r != null)
				r.print(System.out);
		}
		executor.shutdown();
		executor.awaitTermination(5, TimeUnit.SECONDS);
		//Even if we didn't shut down cleanly, we're done.
		System.exit(0);
	}

	private static final class BenchmarkProviderFilterTask implements Runnable {
		private final BenchmarkProvider provider;
		private final Set<String> includedClasses, excludedClasses;
		private final Set<Attribute> includedAttrs, excludedAttrs;
		private final ExecutorCompletionService<Result> executor;
		private BenchmarkProviderFilterTask(BenchmarkProvider provider, Set<String> includedClasses, Set<String> excludedClasses, Set<Attribute> includedAttrs, Set<Attribute> excludedAttrs, ExecutorCompletionService<Result> executor) {
			this.provider = provider;
			this.includedClasses = includedClasses;
			this.excludedClasses = excludedClasses;
			this.includedAttrs = includedAttrs;
			this.excludedAttrs = excludedAttrs;
			this.executor = executor;
		}
		@Override
		public void run() {
			Attribute providerPackageAttr = getPackageAttr(provider.getClass());
			if (providerPackageAttr != null && excludedAttrs.contains(providerPackageAttr))
				return;
			for (Benchmark benchmark : provider) {
				executor.submit(new BenchmarkFilterTask(benchmark, providerPackageAttr,
						includedClasses, excludedClasses, includedAttrs, excludedAttrs, executor), null);
			}
		}
	}

	/**
	 * Filters a benchmark based on the command-line options, optionally
	 * submitting RunTasks.
	 */
	private static final class BenchmarkFilterTask implements Runnable {
		private final Benchmark benchmark;
		private final Attribute providerPackageAttr;
		private final Set<String> includedClasses, excludedClasses;
		private final Set<Attribute> includedAttrs, excludedAttrs;
		private final ExecutorCompletionService<Result> executor;
		private BenchmarkFilterTask(Benchmark benchmark, Attribute providerPackageAttr, Set<String> includedClasses, Set<String> excludedClasses, Set<Attribute> includedAttrs, Set<Attribute> excludedAttrs, ExecutorCompletionService<Result> executor) {
			this.benchmark = benchmark;
			this.providerPackageAttr = providerPackageAttr;
			this.includedClasses = includedClasses;
			this.excludedClasses = excludedClasses;
			this.includedAttrs = includedAttrs;
			this.excludedAttrs = excludedAttrs;
			this.executor = executor;
		}
		@Override
		public void run() {
			//If we exclude APP, SANITY or REGRESSION, we can eliminate some
			//benchmarks without instantiating a graph.
			Attribute packageAttr = getPackageAttr(benchmark.getClass());
			if (packageAttr != null && excludedAttrs.contains(packageAttr))
				return;

			AttributeVisitor visitor = new AttributeVisitor();
			benchmark.instantiate().visit(visitor);
			if (providerPackageAttr != null)
				visitor.attributes.add(providerPackageAttr);
			if (packageAttr != null)
				visitor.attributes.add(packageAttr);

			//Exclusion trumps inclusion, so first check for exclusion.
			if (!Sets.intersection(visitor.attributes, excludedAttrs).isEmpty())
				return;
			for (Class<?> klass : visitor.classes)
				if (excludedClasses.contains(klass.getSimpleName()) || excludedClasses.contains(klass.getCanonicalName()))
					return;

			//Now check inclusion.
			boolean included = false;
			if (!Sets.intersection(visitor.attributes, includedAttrs).isEmpty())
				included = true;
			if (!included)
				for (Class<?> klass : visitor.classes)
					if (includedClasses.contains(klass.getSimpleName()) || includedClasses.contains(klass.getCanonicalName()))
						included = true;
			if (!included)
				return;

			//TODO: checking here isn't great because it's hard to report the
			//exception.
//			if (options.has("check"))
//				benchmark.instantiate().visit(new CheckVisitor());

			StreamCompiler[] compilers = {
				new DebugStreamCompiler(),
				new CompilerStreamCompiler(),
//				new CompilerStreamCompiler().multiplier(10),
//				new CompilerStreamCompiler().multiplier(100),
//				new CompilerStreamCompiler().multiplier(1000),
//				new CompilerStreamCompiler().multiplier(10000),
			};
			for (StreamCompiler sc : compilers)
				for (Dataset input : benchmark.inputs())
					executor.submit(new RunTask(benchmark, input, sc));
		}
	}

	/**
	 * Runs a benchmark with a given dataset and compiler.
	 */
	private static final class RunTask implements Callable<Result> {
		private final Benchmark benchmark;
		private final Dataset dataset;
		private final StreamCompiler compiler;
		private RunTask(Benchmark benchmark, Dataset dataset, StreamCompiler compiler) {
			this.benchmark = benchmark;
			this.dataset = dataset;
			this.compiler = compiler;
		}
		@Override
		public Result call() {
			return run(benchmark, dataset, compiler);
		}
	}

	/**
	 * Runs all the datasets for all the benchmarks in the given provider on the
	 * given compiler.  This entry point is to make the individual provider
	 * classes runnable (call this from main()) for convenience when debugging
	 * an individual benchmark.
	 * @param provider the provider to run benchmarks of
	 * @param compiler the compiler to use
	 */
	public static List<Result> runBenchmarks(BenchmarkProvider provider, StreamCompiler compiler) {
		ImmutableList.Builder<Result> results = ImmutableList.builder();
		for (Benchmark benchmark : provider)
			results.addAll(runBenchmark(benchmark, compiler));
		return results.build();
	}

	/**
	 * Runs all the datasets for the given benchmark on the given compiler.
	 * This entry point is to make the individual benchmark classes runnable
	 * (call this from main()) for convenience when debugging an individual
	 * benchmark.
	 * @param benchmark the benchmark to run
	 * @param compiler the compiler to use
	 */
	public static List<Result> runBenchmark(Benchmark benchmark, StreamCompiler compiler) {
		ImmutableList.Builder<Result> results = ImmutableList.builder();
		for (Dataset input : benchmark.inputs())
			results.add(run(benchmark, input, compiler));
		return results.build();
	}

	private static final long TIMEOUT_DURATION = 1;
	private static final TimeUnit TIMEOUT_UNIT = TimeUnit.MINUTES;
	private static Result run(Benchmark benchmark, Dataset input, StreamCompiler compiler) {
		long compileMillis, runMillis;
		VerifyingOutputBufferFactory verifier = null;
		if (input.output() != null)
			verifier = new VerifyingOutputBufferFactory(input.output());
		try {
			Stopwatch stopwatch = new Stopwatch();
			stopwatch.start();
			CompiledStream stream = compiler.compile(benchmark.instantiate(), input.input(),
					verifier != null ? OutputBufferFactory.wrap(verifier) : Output.blackHole());
			compileMillis = stopwatch.elapsed(TimeUnit.MILLISECONDS);
			stream.awaitDrained(TIMEOUT_DURATION, TIMEOUT_UNIT);
			stopwatch.stop();
			runMillis = stopwatch.elapsed(TimeUnit.MILLISECONDS);
		} catch (TimeoutException ex) {
			return Result.timeout(benchmark, input, compiler);
		} catch (InterruptedException ex) {
			return Result.exception(benchmark, input, compiler, ex);
		} catch (Throwable t) {
			return Result.exception(benchmark, input, compiler, t);
		}
		if (verifier != null && !verifier.buffer.correct())
			return Result.wrongOutput(benchmark, input, compiler, compileMillis, runMillis,
					verifier.buffer.extents, verifier.buffer.missingOutput, verifier.buffer.excessOutput);
		return Result.ok(benchmark, input, compiler, compileMillis, runMillis);
	}

	public static final class Result {
		public static enum Kind {
			OK, WRONG_OUTPUT, EXCEPTION, TIMEOUT;
		}
		private final Kind kind;
		private final Benchmark benchmark;
		private final Dataset dataset;
		private final StreamCompiler compiler;
		private final ImmutableList<Extent> wrongOutput;
		private final ImmutableList<Object> missingOutput, excessOutput;
		private final Throwable throwable;
		private final long compileMillis, runMillis;
		private Result(Kind kind, Benchmark benchmark, Dataset dataset, StreamCompiler compiler, List<Extent> wrongOutput, List<Object> missingOutput, List<Object> excessOutput, Throwable throwable, long compileMillis, long runMillis) {
			this.kind = kind;
			this.benchmark = benchmark;
			this.dataset = dataset;
			this.compiler = compiler;
			this.wrongOutput = wrongOutput == null ? null : ImmutableList.copyOf(wrongOutput);
			this.missingOutput = missingOutput == null ? null : ImmutableList.copyOf(missingOutput);
			this.excessOutput = excessOutput == null ? null : ImmutableList.copyOf(excessOutput);
			this.throwable = throwable;
			this.compileMillis = compileMillis;
			this.runMillis = runMillis;
		}
		private static Result ok(Benchmark benchmark, Dataset dataset, StreamCompiler compiler, long compileMillis, long runMillis) {
			return new Result(Kind.OK, benchmark, dataset, compiler, null, null, null, null, compileMillis, runMillis);
		}
		private static Result wrongOutput(Benchmark benchmark, Dataset dataset, StreamCompiler compiler, long compileMillis, long runMillis, List<Extent> wrongOutput, List<Object> missingOutput, List<Object> excessOutput) {
			return new Result(Kind.WRONG_OUTPUT, benchmark, dataset, compiler, wrongOutput, missingOutput, excessOutput, null, compileMillis, runMillis);
		}
		private static Result exception(Benchmark benchmark, Dataset dataset, StreamCompiler compiler, Throwable throwable) {
			return new Result(Kind.EXCEPTION, benchmark, dataset, compiler, null, null, null, throwable, -1, -1);
		}
		private static Result timeout(Benchmark benchmark, Dataset dataset, StreamCompiler compiler) {
			return new Result(Kind.TIMEOUT, benchmark, dataset, compiler, null, null, null, null, -1, -1);
		}
		public void print(OutputStream stream) {
			print(stream, new HumanResultFormatter());
		}
		public void print(Writer writer) {
			print(writer, new HumanResultFormatter());
		}
		public void print(OutputStream stream, ResultFormatter formatter) {
			print(new OutputStreamWriter(stream, StandardCharsets.UTF_8), formatter);
		}
		public void print(Writer writer, ResultFormatter formatter) {
			print(new PrintWriter(writer), formatter);
		}
		private void print(PrintWriter writer, ResultFormatter formatter) {
			writer.write(formatter.format(this));
			writer.flush();
		}
		@Override
		public String toString() {
			return new HumanResultFormatter().format(this);
		}
	}

	/**
	 * Formats (stringifies) Result objects.
	 */
	public static interface ResultFormatter {
		public String format(Result result);
	}

	/**
	 * Stringifies Result objects in human-readable form.
	 */
	public static final class HumanResultFormatter implements ResultFormatter {
		@Override
		public String format(Result result) {
			StringBuilder sb = new StringBuilder();
			String statusText = "BUG";
			if (result.kind == Result.Kind.OK)
				statusText = String.format("%d ms compile, %d ms run", result.compileMillis, result.runMillis);
			else if (result.kind == Result.Kind.WRONG_OUTPUT)
				statusText = "wrong output";
			else if (result.kind == Result.Kind.EXCEPTION)
				statusText = "failed";
			else if (result.kind == Result.Kind.TIMEOUT)
				statusText = "timed out";
			sb.append(String.format("%s / %s / %s: %s%n", result.compiler, result.benchmark, result.dataset, statusText));

			if (result.kind == Result.Kind.EXCEPTION)
				sb.append(Throwables.getStackTraceAsString(result.throwable));

			if (result.kind == Result.Kind.WRONG_OUTPUT) {
				for (Extent e : result.wrongOutput) {
					sb.append(String.format("wrong output at index %d for %d items%n", e.startIndex, e.size()));
					//TODO: work item by item to line these up?
					sb.append(String.format("  expected: "+e.expected));
					sb.append(String.format("    actual: "+e.actual));
				}
				if (!result.missingOutput.isEmpty()) {
					sb.append(String.format("output ended %d items early%n", result.missingOutput.size()));
					sb.append(String.format("  expected: "+result.missingOutput));
				}
				if (!result.excessOutput.isEmpty()) {
					sb.append(String.format("output contained %d excess items%n", result.excessOutput.size()));
					sb.append(String.format("  actual: "+result.excessOutput));
				}
			}

			return sb.toString();
		}
	}

	/**
	 * An Extent represents a range of wrong output starting at a specified
	 * index.  (The name comes from filesystems.)
	 */
	private static final class Extent {
		private final long startIndex;
		private final List<Object> expected, actual;
		private Extent(long startIndex, List<Object> expected, List<Object> actual) {
			this.startIndex = startIndex;
			this.expected = expected;
			this.actual = actual;
		}
		public int size() {
			assert expected.size() == actual.size() : expected.size() + " " + actual.size();
			return expected.size();
		}
	}

	private static final class VerifyingOutputBufferFactory extends OutputBufferFactory {
		private final Input<Object> expectedOutput;
		private VerifyingBuffer buffer;
		private VerifyingOutputBufferFactory(Input<Object> expectedOutput) {
			this.expectedOutput = expectedOutput;
			this.buffer = new VerifyingBuffer();
		}

		@Override
		public Buffer createWritableBuffer(int writerMinSize) {
			return buffer;
		}

		private final class VerifyingBuffer extends AbstractWriteOnlyBuffer {
			private final Buffer expected = InputBufferFactory.unwrap(expectedOutput).createReadableBuffer(42);
			private final List<Extent> extents = new ArrayList<>();
			private final List<Object> excessOutput = new ArrayList<>();
			private final List<Object> missingOutput = new ArrayList<>();
			private long index = 0;
			private Extent current = null;
			@Override
			public boolean write(Object t) {
				if (expected.size() == 0) {
					excessOutput.add(t);
				} else {
					Object e = expected.read();
					if (!Objects.equals(e, t)) {
						if (current == null) {
							current = new Extent(index, new ArrayList<>(), new ArrayList<>());
							extents.add(current);
						}
						current.expected.add(e);
						current.actual.add(t);
					}
				}
				++index;
				return true;
			}
			public void finish() {
				while (expected.size() > 0)
					missingOutput.add(expected.read());
			}
			public boolean correct() {
				return excessOutput.isEmpty() && missingOutput.isEmpty() && extents.isEmpty();
			}
		}
	}

	private static final class AttributeVisitor extends StreamVisitor {
		private final EnumSet<Attribute> attributes = EnumSet.noneOf(Attribute.class);
		private final Set<Class<?>> classes = new HashSet<>();
		@Override
		public void beginVisit() {
		}
		@Override
		public void visitFilter(Filter<?, ?> filter) {
			visitWorker(filter);
		}
		@Override
		public boolean enterPipeline(Pipeline<?, ?> pipeline) {
			classes.addAll(ReflectionUtils.getAllSupertypes(pipeline.getClass()));
			return true;
		}
		@Override
		public void exitPipeline(Pipeline<?, ?> pipeline) {
		}
		@Override
		public boolean enterSplitjoin(Splitjoin<?, ?> splitjoin) {
			classes.addAll(ReflectionUtils.getAllSupertypes(splitjoin.getClass()));
			return true;
		}
		@Override
		public void visitSplitter(Splitter<?, ?> splitter) {
			visitWorker(splitter);
		}
		@Override
		public boolean enterSplitjoinBranch(OneToOneElement<?, ?> element) {
			return true;
		}
		@Override
		public void exitSplitjoinBranch(OneToOneElement<?, ?> element) {
		}
		@Override
		public void visitJoiner(Joiner<?, ?> joiner) {
			visitWorker(joiner);
		}
		@Override
		public void exitSplitjoin(Splitjoin<?, ?> splitjoin) {
		}
		private void visitWorker(Worker<?, ?> worker) {
			classes.addAll(ReflectionUtils.getAllSupertypes(worker.getClass()));
			if (worker instanceof StatefulFilter)
				attributes.add(Attribute.STATEFUL);
			for (Rate r : worker.getPopRates())
				if (!r.isFixed())
					attributes.add(Attribute.DYNAMIC);
			for (Rate r : worker.getPushRates())
				if (!r.isFixed())
					attributes.add(Attribute.DYNAMIC);
			for (int i = 0; i < worker.getPeekRates().size(); ++i) {
				Rate peek = worker.getPeekRates().get(i);
				Rate pop = worker.getPopRates().get(i);
				if (peek.max() == Rate.DYNAMIC || peek.max() > pop.max())
					attributes.add(Attribute.PEEKING);
			}
		}
		@Override
		public void endVisit() {
			if (!attributes.contains(Attribute.DYNAMIC))
				attributes.contains(Attribute.STATIC);
			if (!attributes.contains(Attribute.STATEFUL))
				attributes.contains(Attribute.STATELESS);
		}
	}

	private static Attribute getPackageAttr(Class<?> klass) {
		String name = klass.getName();
		for (Map.Entry<String, Attribute> entry : PACKAGE_TO_ATTRIBUTE.entrySet())
			if (name.startsWith(entry.getKey()))
				return entry.getValue();
		return null;
	}

	/**
	 * A BenchmarkProvider providing Benchmarks directly registered with
	 * @ServiceProvider(Benchmark.class). This is in the test package, not apps,
	 * sanity or regression, so it does not affect attributes.
	 * <p/>
	 * This class is public so ServiceLoader can instantiate it.
	 */
	@ServiceProvider(BenchmarkProvider.class)
	public static final class ServiceBenchmarkProvider implements BenchmarkProvider {
		private final ServiceLoader<Benchmark> loader = ServiceLoader.load(Benchmark.class);
		@Override
		public Iterator<Benchmark> iterator() {
			return new SkipMissingServicesIterator<>(loader.iterator());
		}
	}
}
