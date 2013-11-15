package edu.mit.streamjit.impl.compiler2;

import static com.google.common.base.Preconditions.*;
import com.google.common.base.Predicate;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.util.Combinators;
import static edu.mit.streamjit.util.LookupUtils.findStatic;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Compiler IR for a fused group of workers (what used to be called StreamNode).
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 9/22/2013
 */
public class ActorGroup implements Comparable<ActorGroup> {
	private ImmutableSet<Actor> actors;
	private ImmutableMap<Actor, Integer> schedule;
	private ActorGroup(ImmutableSet<Actor> actors) {
		this.actors = actors;
		for (Actor a : actors)
			a.setGroup(this);
	}

	public static ActorGroup of(Actor actor) {
		assert actor.group() == null : actor.group();
		return new ActorGroup(ImmutableSet.of(actor));
	}

	public static ActorGroup fuse(ActorGroup first, ActorGroup second) {
		return new ActorGroup(ImmutableSet.<Actor>builder().addAll(first.actors()).addAll(second.actors()).build());
	}

	public void remove(Actor a) {
		assert actors.contains(a) : a;
		actors = ImmutableSet.copyOf(Sets.difference(actors, ImmutableSet.of(a)));
		schedule = ImmutableMap.copyOf(Maps.difference(schedule, ImmutableMap.of(a, 0)).entriesOnlyOnLeft());
	}

	public ImmutableSet<Actor> actors() {
		return actors;
	}

	public boolean isTokenGroup() {
		for (Actor a : actors())
			if (a instanceof TokenActor)
				return true;
		return false;
	}

	public int id() {
		return Collections.min(actors()).id();
	}

	public boolean isPeeking() {
		for (Actor a : actors())
			if (a.isPeeking())
				return true;
		return false;
	}

	public boolean isStateful() {
		for (Actor a : actors())
			if (a instanceof WorkerActor && ((WorkerActor)a).archetype().isStateful())
				return true;
		return false;
	}

	public Set<Storage> inputs() {
		ImmutableSet.Builder<Storage> builder = ImmutableSet.builder();
		for (Actor a : actors())
			for (Storage s : a.inputs())
				for (Actor producer : s.upstream())
					if (producer.group() != this)
						builder.add(s);
		return builder.build();
	}

	public Set<Storage> outputs() {
		ImmutableSet.Builder<Storage> builder = ImmutableSet.builder();
		for (Actor a : actors())
			for (Storage s : a.outputs())
				for (Actor consumer : s.downstream())
					if (consumer.group() != this)
						builder.add(s);
		return builder.build();
	}

	public Set<Storage> internalEdges() {
		return Sets.filter(allEdges(), new Predicate<Storage>() {
			@Override
			public boolean apply(Storage input) {
				return Iterables.getOnlyElement(input.upstream()).group() == ActorGroup.this &&
						Iterables.getOnlyElement(input.downstream()).group() == ActorGroup.this;
			}
		});
	}

	private Set<Storage> allEdges() {
		ImmutableSet.Builder<Storage> builder = ImmutableSet.builder();
		for (Actor a : actors) {
			builder.addAll(a.inputs());
			builder.addAll(a.outputs());
		}
		return builder.build();
	}

	public Set<ActorGroup> predecessorGroups() {
		ImmutableSet.Builder<ActorGroup> builder = ImmutableSet.builder();
		for (Actor a : actors)
			for (Storage s : a.inputs())
				for (Actor b : s.upstream())
					if (b.group() != this)
						builder.add(b.group());
		return builder.build();
	}

	public Set<ActorGroup> successorGroups() {
		ImmutableSet.Builder<ActorGroup> builder = ImmutableSet.builder();
		for (Actor a : actors)
			for (Storage s : a.outputs())
				for (Actor b : s.downstream())
					if (b.group() != this)
						builder.add(b.group());
		return builder.build();
	}

	public ImmutableMap<Actor, Integer> schedule() {
		checkState(schedule != null, "schedule not yet initialized");
		return schedule;
	}

	public void setSchedule(ImmutableMap<Actor, Integer> schedule) {
		checkState(this.schedule == null, "already initialized schedule");
		for (Actor a : actors())
			checkArgument(schedule.containsKey(a), "schedule doesn't contain actor "+a);
		this.schedule = schedule;
	}

	/**
	 * Returns a map mapping each output Storage to the set of physical indices
	 * read in that Storage during the given ActorGroup iteration.
	 * @param iteration the iteration to simulate
	 * @return a map of read physical indices
	 */
	public Map<Storage, Set<Integer>> reads(int iteration) {
		Map<Storage, Set<Integer>> retval = new HashMap<>(outputs().size());
		for (Storage s : inputs())
			retval.put(s, new HashSet<Integer>());

		for (Actor a : actors()) {
			int begin = schedule.get(a) * iteration, end = schedule.get(a) * (iteration + 1);
			for (int input = 0; input < a.inputs().size(); ++input)
				if (!a.inputs().get(input).isInternal()) {
					//In each iteration, our index starts at however many items
					//we've previously popped, and goes until the elements we pop
					//or peek in this iteration, whichever is greater.
					int pop = a.pop(input), read = Math.max(pop, a.peek(input));
					for (int iter = begin; iter < end; ++iter)
						for (int idx = pop * iter; idx < (pop * iter) + read; ++idx)
							retval.get(a.inputs().get(input)).add(a.translateInputIndex(input, idx));
				}
		}
		return retval;
	}

	/**
	 * Returns a map mapping each output Storage to the set of physical indices
	 * written in that Storage during the given ActorGroup iteration.
	 * @param iteration the iteration to simulate
	 * @return a map of written physical indices
	 */
	public Map<Storage, Set<Integer>> writes(int iteration) {
		Map<Storage, Set<Integer>> retval = new HashMap<>(outputs().size());
		for (Storage s : outputs())
			retval.put(s, new HashSet<Integer>());

		for (Actor a : actors()) {
			int begin = schedule.get(a) * iteration, end = schedule.get(a) * (iteration + 1);
			for (int output = 0; output < a.outputs().size(); ++output)
				if (!a.outputs().get(output).isInternal()) {
					int push = a.push(output);
					for (int iter = begin; iter < end; ++iter)
						for (int idx = push * iter; idx < push * (iter+1); ++idx)
							retval.get(a.outputs().get(output)).add(a.translateOutputIndex(output, idx));
				}
		}
		return retval;
	}

	/**
	 * Returns a void->void MethodHandle that will run this ActorGroup for the
	 * given iterations using the given ConcreteStorage instances.
	 * @param iterations the range of iterations to run for
	 * @param storage the storage being used
	 * @return a void->void method handle
	 */
	public MethodHandle specialize(Range<Integer> iterations, Map<Storage, ConcreteStorage> storage) {
		//TokenActors are special.
		assert !isTokenGroup() : actors();

		/**
		 * Compute the read and write method handles for each Actor. These don't
		 * depend on the iteration, so we can bind and reuse them.
		 */
		Map<Actor, MethodHandle> withRWHandlesBound = new HashMap<>();
		for (Actor a : actors()) {
			WorkerActor wa = (WorkerActor)a;
			MethodHandle specialized = wa.archetype().specialize(wa);

			assert a.inputs().size() > 0 : a;
			MethodHandle read;
			if (wa.worker() instanceof Joiner) {
				MethodHandle[] table = new MethodHandle[a.inputs().size()];
				for (int i = 0; i < a.inputs().size(); i++)
					table[i] = MethodHandles.filterArguments(storage.get(a.inputs().get(i)).readHandle(),
							0, a.inputIndexFunctions().get(i));
				read = Combinators.tableswitch(table);
			} else read = MethodHandles.filterArguments(storage.get(a.inputs().get(0)).readHandle(),
					0, a.inputIndexFunctions().get(0));

			assert a.outputs().size() > 0 : a;
			MethodHandle write;
			if (wa.worker() instanceof Splitter) {
				MethodHandle[] table = new MethodHandle[a.outputs().size()];
				for (int i = 0; i < a.outputs().size(); ++i)
					table[i] = MethodHandles.filterArguments(storage.get(a.outputs().get(i)).writeHandle(),
							0, a.outputIndexFunctions().get(i));
				write = Combinators.tableswitch(table);
			} else write = MethodHandles.filterArguments(storage.get(a.outputs().get(0)).writeHandle(),
					0, a.outputIndexFunctions().get(0));

			withRWHandlesBound.put(wa, specialized.bindTo(read).bindTo(write));
		}

		/**
		 * Compute the initial read/write indices for each iteration, then bind
		 * them together in sequence.  (We could also move the computation
		 * inside the handle, but I think leaving everything explicit is better.
		 * We could also bytecode these constants and invoke the method handle,
		 * if bytecode gives the JVM more visibility.)
		 *
		 * TODO: loops for splitters and joiners.  Will need to copy the array
		 * before the loop, but can rely on the work method to update it (note
		 * this changes the work method!)
		 *
		 * TODO: currently we have one handle per iteration, which isn't great
		 * either.  should be an outer loop somehow.
		 */
		List<MethodHandle> handles = new ArrayList<>();
		for (int iteration : ContiguousSet.create(iterations, DiscreteDomain.integers())) {
			for (Actor a : ImmutableSortedSet.copyOf(actors())) {
				MethodHandle base = withRWHandlesBound.get(a);
				if (((WorkerActor)a).worker() instanceof Filter)
					handles.addAll(filterLoopSubiteration(a, base, iteration));
				else
					handles.addAll(genericSubiteration(a, base, iteration));
			}
		}
		return Combinators.semicolon(handles);
	}

	/**
	 * Implements subiterations of the given actor.  This handles all actors but
	 * generates lots of method handles (i.e., uses lots of memory).
	 * @param a the actor
	 * @param base the base method handle (with read and write handles bound)
	 * @param iteration the current ActorGroup iteration
	 * @return handle(s) implementing subiterations of the given actor
	 */
	private List<MethodHandle> genericSubiteration(Actor a, MethodHandle base, int iteration) {
		ImmutableList.Builder<MethodHandle> handles = ImmutableList.builder();
		int subiterations = schedule.get(a);
		for (int i = iteration*subiterations; i < (iteration+1)*subiterations; ++i) {
			MethodHandle next = base;
			if (next.type().parameterType(0).equals(int.class)) {
				assert a.inputs().size() == 1;
				next = MethodHandles.insertArguments(next, 0, i * a.pop(0));
			} else {
				int[] readIndices = new int[a.inputs().size()];
				for (int m = 0; m < a.inputs().size(); ++m)
					readIndices[m] = i * a.pop(m);
				next = MethodHandles.insertArguments(next, 0, readIndices);
			}

			if (next.type().parameterType(0).equals(int.class)) {
				assert a.outputs().size() == 1;
				next = MethodHandles.insertArguments(next, 0, i * a.push(0));
			} else {
				int[] writeIndices = new int[a.outputs().size()];
				for (int m = 0; m < a.outputs().size(); ++m)
					writeIndices[m] = i * a.push(m);
				next = MethodHandles.insertArguments(next, 0, writeIndices);
			}
			handles.add(next);
		}
		return handles.build();
	}

	private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
	private static final MethodHandle FILTER_LOOP = findStatic(LOOKUP, ActorGroup.class, "_filterLoop", void.class, MethodHandle.class, int.class, int.class, int.class, int.class);
	private List<MethodHandle> filterLoopSubiteration(Actor a, MethodHandle base, int iteration) {
		return ImmutableList.of(MethodHandles.insertArguments(FILTER_LOOP, 0, base, iteration, schedule.get(a), a.pop(0), a.push(0)));
	}

	/**
	 * The loop combinator for a filter work function.
	 */
	private static void _filterLoop(MethodHandle work, int iteration, int subiterations, int pop, int push) throws Throwable {
		for (int i = iteration*subiterations; i < (iteration+1)*subiterations; ++i)
			work.invokeExact(i * pop, i * push);
	}

	/**
	 * This is inconsistent with equals, but we should never have two distinct
	 * ActorGroup objects with the same id, so we'll never notice the
	 * inconsistency.  (We used to have equals() and hashCode() by id, but actor
	 * removal would then change the hash code and screw up maps.)
	 */
	@Override
	public int compareTo(ActorGroup o) {
		return Integer.compare(id(), o.id());
	}
}
