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
package edu.mit.streamjit.test;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;
import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.DuplicateSplitter;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.IllegalStreamGraphException;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Rate;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.StreamElement;
import edu.mit.streamjit.api.UnbalancedSplitjoinException;
import edu.mit.streamjit.impl.common.CheckVisitor;
import edu.mit.streamjit.impl.common.PrintStreamVisitor;
import edu.mit.streamjit.impl.common.TestFilters;
import edu.mit.streamjit.impl.common.TestFilters.Adder;
import edu.mit.streamjit.impl.common.TestFilters.ArrayHasher;
import edu.mit.streamjit.impl.common.TestFilters.ArrayListHasher;
import edu.mit.streamjit.impl.common.TestFilters.Batcher;
import edu.mit.streamjit.impl.common.TestFilters.Multiplier;
import edu.mit.streamjit.impl.common.TestFilters.PeekingAdder;
import edu.mit.streamjit.impl.common.TestFilters.StatefulAdder;
import edu.mit.streamjit.impl.common.TestFilters.StatefulMultiplier;
import edu.mit.streamjit.impl.compiler.CompilerStreamCompiler;
import edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;
import edu.mit.streamjit.impl.interp.InterpreterStreamCompiler;
import edu.mit.streamjit.util.ConstructorSupplier;
import edu.mit.streamjit.util.ReflectionUtils;
import edu.mit.streamjit.util.Template;
import edu.mit.streamjit.util.ilpsolve.InfeasibleSystemException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

/**
 * Generates random streams.
 *
 * TODO: this should verify DrainData, which seems to require working at the
 * blob level, or surfacing DrainData somewhere.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 7/26/2013
 */
public final class StreamFuzzer {
	public interface FuzzElement {
		public OneToOneElement<Integer, Integer> instantiate();
		public String toJava();
		@Override
		public boolean equals(Object other);
		@Override
		public int hashCode();
	}

	private static final int MAX_DEPTH = 5;
	public static FuzzElement generate() {
		return makeStream(MAX_DEPTH);
	}

	private static final Random rng = new Random();
	private static final int FILTER_PROB = 50, PIPELINE_PROB = 25, SPLITJOIN_PROB = 25;
	private static FuzzElement makeStream(int depthLimit) {
		int r = rng.nextInt(FILTER_PROB + PIPELINE_PROB + SPLITJOIN_PROB);
		if (depthLimit == 0 || r < FILTER_PROB) {
			return makeFilter();
		} else if (r < FILTER_PROB + PIPELINE_PROB) {
			return makePipeline(depthLimit);
		} else if (r < FILTER_PROB + PIPELINE_PROB + SPLITJOIN_PROB) {
			return makeSplitjoin(depthLimit);
		} else
			throw new AssertionError(r);
	}

	private static final ImmutableList<FuzzFilter> FILTERS = ImmutableList.<FuzzFilter>builder()
			.add(new FuzzFilter(Identity.class, ImmutableList.of()))
			.add(new FuzzFilter(Adder.class, ImmutableList.of(1)))
			.add(new FuzzFilter(Adder.class, ImmutableList.of(20)))
			.add(new FuzzFilter(Multiplier.class, ImmutableList.of(2)))
			.add(new FuzzFilter(Multiplier.class, ImmutableList.of(3)))
			.add(new FuzzFilter(Multiplier.class, ImmutableList.of(100)))
			.add(new FuzzFilter(Batcher.class, ImmutableList.of(2)))
			.add(new FuzzFilter(Batcher.class, ImmutableList.of(10)))
			.add(new FuzzFilter(ArrayHasher.class, ImmutableList.of(1)))
			.add(new FuzzFilter(ArrayHasher.class, ImmutableList.of(2)))
			.add(new FuzzFilter(ArrayHasher.class, ImmutableList.of(3)))
			.add(new FuzzFilter(ArrayListHasher.class, ImmutableList.of(1)))
			.add(new FuzzFilter(ArrayListHasher.class, ImmutableList.of(2)))
			.add(new FuzzFilter(ArrayListHasher.class, ImmutableList.of(3)))
			.add(new FuzzFilter(PeekingAdder.class, ImmutableList.of(3)))
			.add(new FuzzFilter(PeekingAdder.class, ImmutableList.of(10)))
			.add(new FuzzFilter(StatefulAdder.class, ImmutableList.of(1)))
			.add(new FuzzFilter(StatefulAdder.class, ImmutableList.of(20)))
			.add(new FuzzFilter(StatefulMultiplier.class, ImmutableList.of(2)))
			.add(new FuzzFilter(StatefulMultiplier.class, ImmutableList.of(3)))
			.add(new FuzzFilter(StatefulMultiplier.class, ImmutableList.of(100)))
			.build();
	private static FuzzFilter makeFilter() {
		return FILTERS.get(rng.nextInt(FILTERS.size()));
	}

	private static final int MAX_PIPELINE_LENGTH = 5;
	private static FuzzPipeline makePipeline(int depthLimit) {
		int length = rng.nextInt(MAX_PIPELINE_LENGTH) + 1;
		ImmutableList.Builder<FuzzElement> elements = ImmutableList.builder();
		for (int i = 0; i < length; ++i)
			elements.add(makeStream(depthLimit - 1));
		return new FuzzPipeline(elements.build());
	}

	private static final int MAX_SPLITJOIN_BRANCHES = 5;
	private static FuzzSplitjoin makeSplitjoin(int depthLimit) {
		CheckVisitor cv = new CheckVisitor();
		while (true) {
			try {
				int numBranches = rng.nextInt(MAX_SPLITJOIN_BRANCHES) + 1;
				ImmutableList.Builder<FuzzElement> branches = ImmutableList.builder();
				for (int i = 0; i < numBranches; ++i)
					branches.add(makeStream(depthLimit - 1));
				FuzzSplitjoin sj = new FuzzSplitjoin(makeSplitter(), makeJoiner(), branches.build());
				sj.instantiate().visit(cv);
				return sj;
			} catch (UnbalancedSplitjoinException ex) {}
		}
	}

	private static final ImmutableList<FuzzSplitter> SPLITTERS = ImmutableList.<FuzzSplitter>builder()
			.add(new FuzzSplitter(RoundrobinSplitter.class, ImmutableList.of()))
			.add(new FuzzSplitter(RoundrobinSplitter.class, ImmutableList.of(2)))
			.add(new FuzzSplitter(RoundrobinSplitter.class, ImmutableList.of(3)))
			.add(new FuzzSplitter(RoundrobinSplitter.class, ImmutableList.of(4)))
			.add(new FuzzSplitter(DuplicateSplitter.class, ImmutableList.of()))
			.build();
	private static FuzzSplitter makeSplitter() {
		return SPLITTERS.get(rng.nextInt(SPLITTERS.size()));
	}

	private static FuzzJoiner makeJoiner() {
		return new FuzzJoiner(RoundrobinJoiner.class, ImmutableList.of());
	}

	private static final com.google.common.base.Joiner ARG_JOINER = com.google.common.base.Joiner.on(",\n");
	private static class FuzzStreamElement<T extends StreamElement<Integer, Integer>> {
		private final Class<? extends T> filterClass;
		private final ImmutableList<?> arguments;
		private final Supplier<? extends T> supplier;
		protected FuzzStreamElement(Class<? extends T> filterClass, ImmutableList<? extends Object> arguments) {
			this.filterClass = filterClass;
			this.arguments = arguments;
			this.supplier = new ConstructorSupplier<>(filterClass, arguments);
		}
		public T instantiate() {
			return supplier.get();
		}
		public String toJava() {
			return supplier.toString();
		}
		@Override
		public boolean equals(Object obj) {
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final FuzzStreamElement<?> other = (FuzzStreamElement<?>)obj;
			if (!Objects.equals(this.filterClass, other.filterClass))
				return false;
			if (!Objects.equals(this.arguments, other.arguments))
				return false;
			return true;
		}
		@Override
		public int hashCode() {
			int hash = 7;
			hash = 41 * hash + Objects.hashCode(this.filterClass);
			hash = 41 * hash + Objects.hashCode(this.arguments);
			return hash;
		}
	}

	private static final class FuzzFilter extends FuzzStreamElement<Filter<Integer, Integer>> implements FuzzElement {
		@SuppressWarnings({"unchecked","rawtypes"})
		private FuzzFilter(Class<? extends Filter> filterClass, ImmutableList<? extends Object> arguments) {
			super((Class<Filter<Integer, Integer>>)filterClass, arguments);
		}
		@Override
		public Filter<Integer, Integer> instantiate() {
			return super.instantiate();
		}
		//use inherited equals()/hashCode()
	}

	private static final class FuzzPipeline implements FuzzElement {
		private final ImmutableList<FuzzElement> elements;
		private FuzzPipeline(ImmutableList<FuzzElement> elements) {
			this.elements = elements;
		}
		@Override
		public Pipeline<Integer, Integer> instantiate() {
			Pipeline<Integer, Integer> pipeline = new Pipeline<>();
			for (FuzzElement e : elements)
				pipeline.add(e.instantiate());
			return pipeline;
		}
		@Override
		public String toJava() {
			List<String> args = new ArrayList<>(elements.size());
			for (FuzzElement e : elements)
				args.add(e.toJava());
			return "new Pipeline(new OneToOneElement[]{\n" + ARG_JOINER.join(args) + "\n})";
		}
		@Override
		public boolean equals(Object obj) {
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final FuzzPipeline other = (FuzzPipeline)obj;
			if (!Objects.equals(this.elements, other.elements))
				return false;
			return true;
		}
		@Override
		public int hashCode() {
			int hash = 5;
			hash = 59 * hash + Objects.hashCode(this.elements);
			return hash;
		}
	}

	/**
	 * Can't implement FuzzElement because Splitter isn't a OneToOneElement, but
	 * can still share the instantiation code.
	 */
	private static final class FuzzSplitter extends FuzzStreamElement<Splitter<Integer, Integer>> {
		@SuppressWarnings({"unchecked","rawtypes"})
		private FuzzSplitter(Class<? extends Splitter> filterClass, ImmutableList<? extends Object> arguments) {
			super((Class<Splitter<Integer, Integer>>)filterClass, arguments);
		}
		@Override
		public Splitter<Integer, Integer> instantiate() {
			return super.instantiate();
		}
		//use inherited equals()/hashCode()
	}

	/**
	 * See comments on FuzzSplitter.
	 */
	private static final class FuzzJoiner extends FuzzStreamElement<Joiner<Integer, Integer>> {
		@SuppressWarnings({"unchecked","rawtypes"})
		private FuzzJoiner(Class<? extends Joiner> filterClass, ImmutableList<? extends Object> arguments) {
			super((Class<Joiner<Integer, Integer>>)filterClass, arguments);
		}
		@Override
		public Joiner<Integer, Integer> instantiate() {
			return super.instantiate();
		}
		//use inherited equals()/hashCode()
	}

	private static final class FuzzSplitjoin implements FuzzElement {
		private final FuzzSplitter splitter;
		private final FuzzJoiner joiner;
		private final ImmutableList<FuzzElement> branches;
		private FuzzSplitjoin(FuzzSplitter splitter, FuzzJoiner joiner, ImmutableList<FuzzElement> branches) {
			this.splitter = splitter;
			this.joiner = joiner;
			this.branches = branches;
		}
		@Override
		public OneToOneElement<Integer, Integer> instantiate() {
			Splitjoin<Integer, Integer> splitjoin = new Splitjoin<>(splitter.instantiate(), joiner.instantiate());
			for (FuzzElement e : branches)
				splitjoin.add(e.instantiate());
			return splitjoin;
		}
		@Override
		public String toJava() {
			List<String> args = new ArrayList<>(branches.size());
			for (FuzzElement e : branches)
				args.add(e.toJava());
			return "new Splitjoin(" + splitter.toJava() + ", " + joiner.toJava()+", new OneToOneElement[]{\n" + ARG_JOINER.join(args) + "\n})";
		}
		@Override
		public boolean equals(Object obj) {
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final FuzzSplitjoin other = (FuzzSplitjoin)obj;
			if (!Objects.equals(this.splitter, other.splitter))
				return false;
			if (!Objects.equals(this.joiner, other.joiner))
				return false;
			if (!Objects.equals(this.branches, other.branches))
				return false;
			return true;
		}
		@Override
		public int hashCode() {
			int hash = 7;
			hash = 71 * hash + Objects.hashCode(this.splitter);
			hash = 71 * hash + Objects.hashCode(this.joiner);
			hash = 71 * hash + Objects.hashCode(this.branches);
			return hash;
		}
	}

	private static final int INPUT_LENGTH = 1000;
	private static List<Integer> run(FuzzElement element, StreamCompiler compiler) throws InterruptedException {
		OneToOneElement<Integer, Integer> graph = element.instantiate();
		Input<Object> input = Datasets.allIntsInRange(0, INPUT_LENGTH).input();
		List<Integer> retval = new ArrayList<>();
		Output<Integer> output = Output.toCollection(retval);
		@SuppressWarnings("unchecked")
		CompiledStream stream = compiler.compile(graph, (Input)input, output);
		stream.awaitDrained();
		return retval;
	}

	private static final ImmutableSet<Class<?>> ignoredExceptions = ImmutableSet.<Class<?>>of(
			InfeasibleSystemException.class
			);
	public static void main(String[] args) throws InterruptedException, IOException {
		StreamCompiler debugSC = new InterpreterStreamCompiler();
		StreamCompiler compilerSC = new Compiler2StreamCompiler();
		Set<FuzzElement> completedCases = new HashSet<>();
		int generated;
		int duplicatesSkipped = 0;
		Multiset<Class<?>> ignored = HashMultiset.create(ignoredExceptions.size());
		int failures = 0, successes = 0;
		next_case: for (generated = 0; true; ++generated) {
			FuzzElement fuzz = StreamFuzzer.generate();
			if (!completedCases.add(fuzz)) {
				++duplicatesSkipped;
				continue;
			}

			try {
				fuzz.instantiate().visit(new CheckVisitor());
			} catch (IllegalStreamGraphException ex) {
				System.out.println("Fuzzer generated bad test case");
				ex.printStackTrace(System.out);
				fuzz.instantiate().visit(new PrintStreamVisitor(System.out));
			}

			List<Integer> debugOutput = run(fuzz, debugSC);
			List<Integer> compilerOutput = null;
			try {
				compilerOutput = run(fuzz, compilerSC);
			} catch (Throwable ex) {
				for (Throwable t : Throwables.getCausalChain(ex))
					if (ignoredExceptions.contains(t.getClass())) {
						ignored.add(t.getClass());
						continue next_case;
					}
				System.out.println("Compiler failed");
				ex.printStackTrace(System.out);
				//fall into the if below
			}
			if (!debugOutput.equals(compilerOutput)) {
				++failures;
				fuzz.instantiate().visit(new PrintStreamVisitor(System.out));
				System.out.println(fuzz.toJava());
				//TODO: show only elements where they differ
				System.out.println("Debug output: "+debugOutput);
				System.out.println("Compiler output: "+compilerOutput);
				writeRegressionTest(fuzz);
				break;
			} else
				++successes;
			System.out.println(fuzz.hashCode()+" matched");
		}

		System.out.format("Generated %d cases%n", generated);
		System.out.format("  skipped %d duplicates (%f%%)%n", duplicatesSkipped, ((double)duplicatesSkipped)*100/generated);
		for (Class<?> c : ignoredExceptions) {
			int count = ignored.count(c);
			if (count > 0)
				System.out.format("  ignored %d due to %s (%f%%)%n", count, c, ((double)count)*100/generated);
		}
		System.out.format("Ran %d cases (%f%% run rate)%n", successes+failures, ((double)successes+failures)*100/generated);
		System.out.format("  %d succeeded (%f%%)%n", successes, ((double)successes)*100/(successes+failures));
		System.out.format("  %d failed (%f%%)%n", failures, ((double)failures)*100/(successes+failures));
	}

	private static final DateFormat SINCE_FORMAT = new SimpleDateFormat("M/d/yyyy h:mma z");
	private static final DateFormat NAME_FORMAT = new SimpleDateFormat("yyyyMMdd_hhmmss_SSS");
	private static void writeRegressionTest(FuzzElement fuzzTest) throws IOException {
		Template template = Template.from(StreamFuzzer.class.getResourceAsStream("RegressionBenchmark.template"));
		Date now = new Date();
		Map<String, Object> values = new HashMap<>();
		values.put("name", "Reg"+NAME_FORMAT.format(now));
		values.put("since", SINCE_FORMAT.format(now));
		values.put("dataset", "Datasets.allIntsInRange(0, 1000)");
		values.put("instantiate", fuzzTest.toJava());
		values.put("referenceCompiler", "new "+InterpreterStreamCompiler.class.getCanonicalName()+"()");
		values.put("testCompiler", "new "+Compiler2StreamCompiler.class.getCanonicalName()+"()");
		StringBuffer sb = new StringBuffer();
		template.replace(values, sb);

		Path path = Paths.get("src/edu/mit/streamjit/test/regression", values.get("name")+".java");
		Files.write(path, ImmutableList.of(sb.toString()), StandardCharsets.UTF_8, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
	}
}
