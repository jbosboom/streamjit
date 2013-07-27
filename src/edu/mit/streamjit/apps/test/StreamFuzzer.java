package edu.mit.streamjit.apps.test;

import com.google.common.collect.ImmutableList;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.StreamElement;
import edu.mit.streamjit.impl.common.PrintStreamVisitor;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Generates random streams.
 *
 * TODO: This (and all of test/) really doesn't belong under apps/; we should
 * have a separate package for sanity/regression tests (basically anything that
 * isn't real-world).
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 7/26/2013
 */
public final class StreamFuzzer {
	public interface FuzzElement {
		public OneToOneElement<Integer, Integer> instantiate();
		public String toJava();
	}

	public static FuzzElement generate() {
		return makeStream();
	}

	private static final Random rng = new Random();
	private static final int FILTER_PROB = 50, PIPELINE_PROB = 25, SPLITJOIN_PROB = 25;
	private static FuzzElement makeStream() {
		int r = rng.nextInt(FILTER_PROB + PIPELINE_PROB + SPLITJOIN_PROB);
		if (r < FILTER_PROB) {
			return makeFilter();
		} else if (r < FILTER_PROB + PIPELINE_PROB) {
			return makePipeline();
		} else if (r < FILTER_PROB + PIPELINE_PROB + SPLITJOIN_PROB) {
			return makeSplitjoin();
		} else
			throw new AssertionError(r);
	}

	private static FuzzFilter makeFilter() {
		return new FuzzFilter(Identity.class, ImmutableList.of());
	}

	private static final int MAX_PIPELINE_LENGTH = 5;
	private static FuzzPipeline makePipeline() {
		int length = rng.nextInt(MAX_PIPELINE_LENGTH) + 1;
		ImmutableList.Builder<FuzzElement> elements = ImmutableList.builder();
		for (int i = 0; i < length; ++i)
			elements.add(makeStream());
		return new FuzzPipeline(elements.build());
	}

	private static final int MAX_SPLITJOIN_BRANCHES = 5;
	private static FuzzSplitjoin makeSplitjoin() {
		int numBranches = rng.nextInt(MAX_SPLITJOIN_BRANCHES) + 1;
		ImmutableList.Builder<FuzzElement> branches = ImmutableList.builder();
		for (int i = 0; i < numBranches; ++i)
			branches.add(makeStream());
		return new FuzzSplitjoin(makeSplitter(), makeJoiner(), branches.build());
	}

	private static FuzzSplitter makeSplitter() {
		return new FuzzSplitter(RoundrobinSplitter.class, ImmutableList.of());
	}

	private static FuzzJoiner makeJoiner() {
		return new FuzzJoiner(RoundrobinJoiner.class, ImmutableList.of());
	}

	private static final com.google.common.base.Joiner ARG_JOINER = com.google.common.base.Joiner.on(", ");
	private static class FuzzStreamElement<T extends StreamElement<Integer, Integer>> {
		private final Class<? extends T> filterClass;
		private final ImmutableList<Object> arguments;
		protected FuzzStreamElement(Class<? extends T> filterClass, ImmutableList<Object> arguments) {
			this.filterClass = filterClass;
			this.arguments = arguments;
		}
		public T instantiate() {
			@SuppressWarnings("unchecked")
			Constructor<? extends T>[] constructors = (Constructor<T>[])filterClass.getConstructors();
			List<T> retvals = new ArrayList<>();
			List<Throwable> exceptions = new ArrayList<>();
			for (Constructor<? extends T> ctor : constructors)
				try {
					retvals.add(ctor.newInstance(arguments.toArray()));
				} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
					exceptions.add(ex);
				}
			if (retvals.isEmpty())
				throw new AssertionError("Couldn't create a "+filterClass+" from "+arguments+": exceptions "+exceptions);
			if (retvals.size() > 1)
				throw new AssertionError("Creating a "+filterClass+" from "+arguments+" was ambiguous");
			return retvals.get(0);
		}
		public String toJava() {
			//This will generate unchecked code if the filter is generic.
			return "new " + filterClass.getCanonicalName() + "(" + ARG_JOINER.join(arguments)+")";
		}
	}

	private static final class FuzzFilter extends FuzzStreamElement<Filter<Integer, Integer>> implements FuzzElement {
		@SuppressWarnings({"unchecked","rawtypes"})
		private FuzzFilter(Class<? extends Filter> filterClass, ImmutableList<Object> arguments) {
			super((Class<Filter<Integer, Integer>>)filterClass, arguments);
		}
		@Override
		public Filter<Integer, Integer> instantiate() {
			return super.instantiate();
		}
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
			return "new Pipeline(" + ARG_JOINER.join(args) + ")";
		}
	}

	/**
	 * Can't implement FuzzElement because Splitter isn't a OneToOneElement, but
	 * can still share the instantiation code.
	 */
	private static final class FuzzSplitter extends FuzzStreamElement<Splitter<Integer, Integer>> {
		@SuppressWarnings({"unchecked","rawtypes"})
		private FuzzSplitter(Class<? extends Splitter> filterClass, ImmutableList<Object> arguments) {
			super((Class<Splitter<Integer, Integer>>)filterClass, arguments);
		}
		@Override
		public Splitter<Integer, Integer> instantiate() {
			return super.instantiate();
		}
	}

	/**
	 * See comments on FuzzSplitter.
	 */
	private static final class FuzzJoiner extends FuzzStreamElement<Joiner<Integer, Integer>> {
		@SuppressWarnings({"unchecked","rawtypes"})
		private FuzzJoiner(Class<? extends Joiner> filterClass, ImmutableList<Object> arguments) {
			super((Class<Joiner<Integer, Integer>>)filterClass, arguments);
		}
		@Override
		public Joiner<Integer, Integer> instantiate() {
			return super.instantiate();
		}
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
			List<String> args = new ArrayList<>(branches.size()+2);
			args.add(splitter.toJava());
			for (FuzzElement e : branches)
				args.add(e.toJava());
			args.add(joiner.toJava());
			return "new Splitjoin(" + ARG_JOINER.join(args) + ")";
		}
	}

	public static void main(String[] args) {
		FuzzElement fuzz = StreamFuzzer.generate();
		OneToOneElement<Integer, Integer> stream = fuzz.instantiate();
		stream.visit(new PrintStreamVisitor(System.out));
		System.out.println(fuzz.toJava());
	}
}
