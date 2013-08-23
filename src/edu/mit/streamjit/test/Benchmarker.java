package edu.mit.streamjit.test;

import com.google.common.base.Stopwatch;
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
import edu.mit.streamjit.util.Pair;
import edu.mit.streamjit.util.ReflectionUtils;
import edu.mit.streamjit.util.SkipMissingServicesIterator;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
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

	public static void main(String[] args) {
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

		for (BenchmarkProvider provider : ServiceLoader.load(BenchmarkProvider.class)) {
			//If we exclude APP, SANITY or REGRESSION, we can eliminate all the
			//provider's benchmarks at once.
			Attribute providerPackageAttr = getPackageAttr(provider.getClass());
			if (providerPackageAttr != null && excludedAttrs.contains(providerPackageAttr))
				continue;
			next_benchmark: for (Benchmark benchmark : provider) {
				//If we exclude APP, SANITY or REGRESSION, we can eliminate some
				//benchmarks without instantiating a graph.
				Attribute packageAttr = getPackageAttr(benchmark.getClass());
				if (packageAttr != null && excludedAttrs.contains(packageAttr))
					continue next_benchmark;

				AttributeVisitor visitor = new AttributeVisitor();
				benchmark.instantiate().visit(visitor);
				if (providerPackageAttr != null)
					visitor.attributes.add(providerPackageAttr);
				if (packageAttr != null)
					visitor.attributes.add(packageAttr);

				//Exclusion trumps inclusion, so first check for exclusion.
				if (!Sets.intersection(visitor.attributes, excludedAttrs).isEmpty())
					continue next_benchmark;
				for (Class<?> klass : visitor.classes)
					if (excludedClasses.contains(klass.getSimpleName()) || excludedClasses.contains(klass.getCanonicalName()))
						continue next_benchmark;

				//Now check inclusion.
				boolean included = false;
				if (!Sets.intersection(visitor.attributes, includedAttrs).isEmpty())
					included = true;
				if (!included)
					for (Class<?> klass : visitor.classes)
						if (includedClasses.contains(klass.getSimpleName()) || includedClasses.contains(klass.getCanonicalName()))
							included = true;
				if (!included)
					continue next_benchmark;

				if (options.has("check"))
					benchmark.instantiate().visit(new CheckVisitor());

				StreamCompiler[] compilers = {
					new DebugStreamCompiler(),
					new CompilerStreamCompiler(),
	//				new CompilerStreamCompiler().multiplier(10),
	//				new CompilerStreamCompiler().multiplier(100),
	//				new CompilerStreamCompiler().multiplier(1000),
	//				new CompilerStreamCompiler().multiplier(10000),
				};
				for (StreamCompiler sc : compilers)
					runBenchmark(benchmark, sc);
			}
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
	public static void runBenchmarks(BenchmarkProvider provider, StreamCompiler compiler) {
		for (Benchmark benchmark : provider)
			runBenchmark(benchmark, compiler);
	}

	/**
	 * Runs all the datasets for the given benchmark on the given compiler.
	 * This entry point is to make the individual benchmark classes runnable
	 * (call this from main()) for convenience when debugging an individual
	 * benchmark.
	 * @param benchmark the benchmark to run
	 * @param compiler the compiler to use
	 */
	public static void runBenchmark(Benchmark benchmark, StreamCompiler compiler) {
		for (Dataset input : benchmark.inputs())
			run(benchmark, input, compiler);
	}

	private static final long TIMEOUT_DURATION = 1;
	private static final TimeUnit TIMEOUT_UNIT = TimeUnit.MINUTES;
	private static void run(Benchmark benchmark, Dataset input, StreamCompiler compiler) {
		String statusText = null;
		long compileMillis = -1, runMillis = -1;
		Throwable throwable = null;
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
			statusText = "timed out";
		} catch (InterruptedException ex) {
			statusText = "interrupted";
		} catch (Throwable t) {
			statusText = "failed: "+t;
			throwable = t;
		}
		if (statusText == null && verifier != null && !verifier.buffer.correct())
			statusText = "wrong output";
		if (statusText == null)
			statusText = String.format("%d ms compile, %d ms run", compileMillis, runMillis);

		System.out.format("%s / %s / %s: %s%n", compiler, benchmark, input, statusText);
		if (throwable != null)
			throwable.printStackTrace(System.out);
		if (statusText == null && verifier != null && !verifier.buffer.correct()) {
			System.out.println("TODO: generate nice-looking verification output");
		}
	}

	private static final class VerifyingOutputBufferFactory extends OutputBufferFactory {
		private final Input<Object> expectedOutput;
		private VerifyingBuffer buffer;
		private VerifyingOutputBufferFactory(Input<Object> expectedOutput) {
			this.expectedOutput = expectedOutput;
		}

		@Override
		public Buffer createWritableBuffer(int writerMinSize) {
			return (buffer = new VerifyingBuffer());
		}

		private final class VerifyingBuffer extends AbstractWriteOnlyBuffer {
			private long index = 0;
			private final Buffer expected = InputBufferFactory.unwrap(expectedOutput).createReadableBuffer(42);
			private final List<Object> excessOutput = new ArrayList<>();
			private final List<Object> missingOutput = new ArrayList<>();
			//expected, actual
			//TODO: this is high-overhead.
			private final Map<Long, Pair<Object, Object>> wrongOutput = new HashMap<>();
			@Override
			public boolean write(Object t) {
				if (expected.size() == 0) {
					excessOutput.add(t);
				} else {
					Object e = expected.read();
					if (!Objects.equals(e, t))
						wrongOutput.put(index, Pair.make(e, t));
				}
				++index;
				return true;
			}
			public void finish() {
				while (expected.size() > 0)
					missingOutput.add(expected.read());
			}
			public boolean correct() {
				return excessOutput.isEmpty() && missingOutput.isEmpty() && wrongOutput.isEmpty();
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
