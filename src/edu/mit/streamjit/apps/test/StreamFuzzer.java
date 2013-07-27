package edu.mit.streamjit.apps.test;

import com.google.common.collect.ImmutableList;
import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.StreamElement;
import edu.mit.streamjit.impl.common.PrintStreamVisitor;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

	private static FuzzFilter makeFilter() {
		return new FuzzFilter(Identity.class, ImmutableList.of());
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
		int numBranches = rng.nextInt(MAX_SPLITJOIN_BRANCHES) + 1;
		ImmutableList.Builder<FuzzElement> branches = ImmutableList.builder();
		for (int i = 0; i < numBranches; ++i)
			branches.add(makeStream(depthLimit - 1));
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
		private transient Constructor<? extends T> constructor;
		protected FuzzStreamElement(Class<? extends T> filterClass, ImmutableList<Object> arguments) {
			this.filterClass = filterClass;
			this.arguments = arguments;
		}
		public T instantiate() {
			if (constructor == null)
				constructor = findConstructor();
			try {
				return constructor.newInstance(arguments.toArray());
			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
				throw new AssertionError("Failed to instantiate "+constructor+" with "+arguments, ex);
			}
		}
		private Constructor<? extends T> findConstructor() {
			@SuppressWarnings("unchecked")
			Constructor<? extends T>[] constructors = (Constructor<T>[])filterClass.getConstructors();
			List<Constructor<? extends T>> retvals = new ArrayList<>();
			Map<Constructor<? extends T>, Throwable> exceptions = new HashMap<>();
			for (Constructor<? extends T> ctor : constructors)
				try {
					ctor.newInstance(arguments.toArray());
					retvals.add(ctor);
				} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
					exceptions.put(ctor, ex);
				}
			if (retvals.isEmpty())
				throw new AssertionError("Couldn't create a "+filterClass+" from "+arguments+": exceptions "+exceptions);
			if (retvals.size() > 1)
				throw new AssertionError("Creating a "+filterClass+" from "+arguments+" was ambiguous: "+retvals);
			return retvals.get(0);
		}
		public String toJava() {
			//This will generate unchecked code if the filter is generic.
			return "new " + filterClass.getCanonicalName() + "(" + ARG_JOINER.join(arguments)+")";
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final FuzzStreamElement<T> other = (FuzzStreamElement<T>)obj;
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
		private FuzzFilter(Class<? extends Filter> filterClass, ImmutableList<Object> arguments) {
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
			return "new Pipeline(" + ARG_JOINER.join(args) + ")";
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
		private FuzzSplitter(Class<? extends Splitter> filterClass, ImmutableList<Object> arguments) {
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
		private FuzzJoiner(Class<? extends Joiner> filterClass, ImmutableList<Object> arguments) {
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
			List<String> args = new ArrayList<>(branches.size()+2);
			args.add(splitter.toJava());
			for (FuzzElement e : branches)
				args.add(e.toJava());
			args.add(joiner.toJava());
			return "new Splitjoin(" + ARG_JOINER.join(args) + ")";
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
	private static List<Integer> run(FuzzElement element, StreamCompiler compiler) {
		OneToOneElement<Integer, Integer> graph = element.instantiate();
		CompiledStream<Integer, Integer> stream = compiler.compile(graph);
		ImmutableList.Builder<Integer> retval = ImmutableList.builder();
		Integer o;
		for (int i = 0; i < INPUT_LENGTH;) {
			if (stream.offer(i))
				++i;
			while ((o = stream.poll()) != null)
				retval.add(o);
		}

		stream.drain();
		while (!stream.isDrained())
			while ((o = stream.poll()) != null)
				retval.add(o);
		while ((o = stream.poll()) != null)
			retval.add(o);
		return retval.build();
	}

	public static void main(String[] args) {
		FuzzElement fuzz = StreamFuzzer.generate();
		OneToOneElement<Integer, Integer> stream = fuzz.instantiate();
		stream.visit(new PrintStreamVisitor(System.out));
		System.out.println(fuzz.toJava());
		System.out.println(run(fuzz, new DebugStreamCompiler()));
	}
}
