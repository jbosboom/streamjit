/*
 * Copyright (c) 2013-2015 Massachusetts Institute of Technology
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
package edu.mit.streamjit.impl.compiler2;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.util.bytecode.methodhandles.Combinators;
import static edu.mit.streamjit.util.bytecode.methodhandles.LookupUtils.findStatic;
import edu.mit.streamjit.util.bytecode.methodhandles.ProxyFactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

/**
 * Compiler IR for a fused group of workers (what used to be called StreamNode).
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 9/22/2013
 */
public class ActorGroup implements Comparable<ActorGroup> {
	private ImmutableSortedSet<Actor> actors;
	private ImmutableMap<Actor, Integer> schedule;
	private ActorGroup(ImmutableSortedSet<Actor> actors) {
		this.actors = actors;
		for (Actor a : actors)
			a.setGroup(this);
	}

	public static ActorGroup of(Actor actor) {
		assert actor.group() == null : actor.group();
		return new ActorGroup(ImmutableSortedSet.of(actor));
	}

	public static ActorGroup fuse(ActorGroup first, ActorGroup second) {
		return new ActorGroup(ImmutableSortedSet.<Actor>naturalOrder().addAll(first.actors()).addAll(second.actors()).build());
	}

	public void remove(Actor a) {
		assert actors.contains(a) : a;
		actors = ImmutableSortedSet.copyOf(Sets.difference(actors, ImmutableSet.of(a)));
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
				if (!s.isInternal())
					builder.add(s);
		return builder.build();
	}

	public Set<Storage> outputs() {
		ImmutableSet.Builder<Storage> builder = ImmutableSet.builder();
		for (Actor a : actors())
			for (Storage s : a.outputs())
				if (!s.isInternal())
					builder.add(s);
		return builder.build();
	}

	public Set<Storage> internalEdges() {
		return Sets.filter(allEdges(), Storage::isInternal);
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
	 * Returns the physical indices read from the given storage during the given
	 * group iteration.
	 * @param s the storage being read from
	 * @param iteration the group iteration number
	 * @return the physical indices read
	 */
	public ImmutableSortedSet<Integer> reads(Storage s, int iteration) {
		ImmutableSortedSet.Builder<Integer> builder = ImmutableSortedSet.naturalOrder();
		for (Actor a : actors())
			builder.addAll(a.reads(s, Range.closedOpen(iteration * schedule.get(a), (iteration+1) * schedule.get(a))));
		return builder.build();
	}

	/**
	 * Returns the physical indices read from the given storage during the given
	 * group iterations.
	 * @param s the storage being read from
	 * @param iterations the group iterations
	 * @return the physical indices read
	 */
	public ImmutableSortedSet<Integer> reads(Storage s, Range<Integer> iterations) {
		iterations = iterations.canonical(DiscreteDomain.integers());
		ImmutableSortedSet.Builder<Integer> builder = ImmutableSortedSet.naturalOrder();
		for (Actor a : actors())
			builder.addAll(a.reads(s, Range.closedOpen(iterations.lowerEndpoint() * schedule.get(a), iterations.upperEndpoint() * schedule.get(a))));
		return builder.build();
	}

	/**
	 * Returns a map mapping each input Storage to the set of physical indices
	 * read in that Storage during the given ActorGroup iteration.
	 * @param iteration the iteration to simulate
	 * @return a map of read physical indices
	 */
	public ImmutableMap<Storage, ImmutableSortedSet<Integer>> reads(final int iteration) {
		return Maps.toMap(inputs(), (Storage input) -> reads(input, iteration));
	}

	/**
	 * Returns the physical indices written to the given storage during the
	 * given group iteration.
	 * @param s the storage being written to
	 * @param iteration the group iteration number
	 * @return the physical indices written
	 */
	public ImmutableSortedSet<Integer> writes(Storage s, int iteration) {
		ImmutableSortedSet.Builder<Integer> builder = ImmutableSortedSet.naturalOrder();
		for (Actor a : actors())
			builder.addAll(a.writes(s, Range.closedOpen(iteration * schedule.get(a), (iteration+1) * schedule.get(a))));
		return builder.build();
	}

	/**
	 * Returns the physical indices written to the given storage during the
	 * given group iteration.
	 * @param s the storage being written to
	 * @param iterations the group iterations
	 * @return the physical indices written
	 */
	public ImmutableSortedSet<Integer> writes(Storage s, Range<Integer> iterations) {
		ImmutableSortedSet.Builder<Integer> builder = ImmutableSortedSet.naturalOrder();
		for (Actor a : actors())
			builder.addAll(a.writes(s, Range.closedOpen(iterations.lowerEndpoint() * schedule.get(a), iterations.upperEndpoint() * schedule.get(a))));
		return builder.build();
	}

	/**
	 * Returns a map mapping each output Storage to the set of physical indices
	 * written in that Storage during the given ActorGroup iteration.
	 * @param iteration the iteration to simulate
	 * @return a map of written physical indices
	 */
	public ImmutableMap<Storage, ImmutableSortedSet<Integer>> writes(final int iteration) {
		return Maps.toMap(outputs(), (Storage output) -> writes(output, iteration));
	}

	/**
	 * Returns a void->void MethodHandle that will run this ActorGroup for the
	 * given iterations using the given ConcreteStorage instances.
	 * @param iterations the range of iterations to run for
	 * @param storage the storage being used
	 * @return a void->void method handle
	 */
	public MethodHandle specialize(Range<Integer> iterations, Map<Storage, ConcreteStorage> storage,
			BiFunction<MethodHandle[], WorkerActor, MethodHandle> switchFactory,
			int unrollFactor,
			ImmutableTable<Actor, Integer, IndexFunctionTransformer> inputTransformers,
			ImmutableTable<Actor, Integer, IndexFunctionTransformer> outputTransformers) {
		//TokenActors are special.
		assert !isTokenGroup() : actors();

		Map<Actor, MethodHandle> withRWHandlesBound =
				bindActorsToStorage(iterations, storage, switchFactory, inputTransformers, outputTransformers);

		int totalIterations = iterations.upperEndpoint() - iterations.lowerEndpoint();
		unrollFactor = Math.min(unrollFactor, totalIterations);
		int unrolls = (totalIterations/unrollFactor);
		int unrollEndpoint = iterations.lowerEndpoint() + unrolls*unrollFactor;
		MethodHandle overall = Combinators.semicolon(
				makeGroupLoop(Range.closedOpen(iterations.lowerEndpoint(), unrollEndpoint), unrollFactor, withRWHandlesBound),
				makeGroupLoop(Range.closedOpen(unrollEndpoint, iterations.upperEndpoint()), 1, withRWHandlesBound)
		);
		return overall;
	}

	/**
	 * Compute the read and write method handles for each Actor. These don't
	 * depend on the iteration, so we can bind and reuse them.
	 */
	private Map<Actor, MethodHandle> bindActorsToStorage(Range<Integer> iterations, Map<Storage, ConcreteStorage> storage, BiFunction<MethodHandle[], WorkerActor, MethodHandle> switchFactory, ImmutableTable<Actor, Integer, IndexFunctionTransformer> inputTransformers, ImmutableTable<Actor, Integer, IndexFunctionTransformer> outputTransformers) {
		Map<Actor, MethodHandle> withRWHandlesBound = new HashMap<>();
		for (Actor a : actors()) {
			WorkerActor wa = (WorkerActor)a;
			MethodHandle specialized = wa.archetype().specialize(wa);

			assert a.inputs().size() > 0 : a;
			MethodType readHandleType = MethodType.methodType(wa.inputType().getRawType(), int.class);
			MethodHandle read;
			if (wa.worker() instanceof Joiner) {
				MethodHandle[] table = new MethodHandle[a.inputs().size()];
				IntStream.range(0, a.inputs().size()).forEachOrdered(i ->
					table[i] = MethodHandles.filterArguments(storage.get(a.inputs().get(i)).readHandle(), 0,
							inputTransformers.get(a, i).transform(a.inputIndexFunctions().get(i).asHandle(), () -> a.peeks(i, iterations)))
							.asType(readHandleType));
				read = switchFactory.apply(table, wa);
			} else
				read = MethodHandles.filterArguments(storage.get(a.inputs().get(0)).readHandle(), 0,
						inputTransformers.get(a, 0).transform(a.inputIndexFunctions().get(0).asHandle(), () -> a.peeks(0, iterations)))
						.asType(readHandleType);

			assert a.outputs().size() > 0 : a;
			MethodType writeHandleType = MethodType.methodType(void.class, int.class, wa.outputType().getRawType());
			MethodHandle write;
			if (wa.worker() instanceof Splitter) {
				MethodHandle[] table = new MethodHandle[a.outputs().size()];
				IntStream.range(0, a.outputs().size()).forEachOrdered(i ->
					table[i] = MethodHandles.filterArguments(storage.get(a.outputs().get(i)).writeHandle(), 0,
							outputTransformers.get(a, i).transform(a.outputIndexFunctions().get(i).asHandle(), () -> a.pushes(i, iterations)))
							.asType(writeHandleType));
				write = switchFactory.apply(table, wa);
			} else
				write = MethodHandles.filterArguments(storage.get(a.outputs().get(0)).writeHandle(), 0,
						outputTransformers.get(a, 0).transform(a.outputIndexFunctions().get(0).asHandle(), () -> a.pushes(0, iterations)))
						.asType(writeHandleType);

			withRWHandlesBound.put(wa, specialized.bindTo(read).bindTo(write));
		}
		return withRWHandlesBound;
	}

	/**
	 * Make loop handles for each Actor that execute the iteration given as
	 * an argument, then bind them together in an outer loop body that
	 * executes all the iterations.  Before the outer loop we must also
	 * reinitialize the splitter/joiner index arrays to their initial
	 * values.
	 */
	private MethodHandle makeGroupLoop(Range<Integer> iterations, int unrollFactor, Map<Actor, MethodHandle> withRWHandlesBound) {
		if (iterations.isEmpty()) return Combinators.nop();
		List<MethodHandle> loopHandles = new ArrayList<>(actors().size());
		Map<int[], int[]> requiredCopies = new LinkedHashMap<>();
		for (Actor a : actors())
			loopHandles.add(makeWorkerLoop((WorkerActor)a, withRWHandlesBound.get(a), unrollFactor, iterations.lowerEndpoint(), requiredCopies));
		MethodHandle groupLoop = MethodHandles.insertArguments(OVERALL_GROUP_LOOP, 0,
				Combinators.semicolon(loopHandles), iterations.lowerEndpoint(), iterations.upperEndpoint(), unrollFactor);
		if (!requiredCopies.isEmpty()) {
			int[][] copies = new int[requiredCopies.size()*2][];
			int i = 0;
			for (Map.Entry<int[], int[]> e : requiredCopies.entrySet()) {
				copies[i++] = e.getKey();
				copies[i++] = e.getValue();
			}
			groupLoop = Combinators.semicolon(
					MethodHandles.insertArguments(REINITIALIZE_ARRAYS, 0, (Object)copies),
					groupLoop);
		}
		return groupLoop;
	}

	/**
	 * Makes the loop for the given actor, which implements group executions
	 * based on the unroll factor.
	 * @param a the actor
	 * @param base the specialized work method with read/write handles bound;
	 * takes two int or int[] parameters
	 * @param unrollFactor the number of group iterations to execute
	 * @param firstIteration the first iteration to execute, for computing the
	 * initial contents of index arrays
	 * @param requiredCopies accumulates the copies required to reinitialize the
	 * index arrays
	 * @return a MethodHandle taking one int parameter
	 */
	private MethodHandle makeWorkerLoop(WorkerActor a, MethodHandle base, int unrollFactor, int firstIteration, Map<int[], int[]> requiredCopies) {
		int subiterations = schedule.get(a);
		Object pop, push;
		if (base.type().parameterType(0).equals(int.class)) {
			assert a.inputs().size() == 1;
			pop = a.pop(0);
		} else {
			int[] readIndices = new int[a.inputs().size()];
			for (int m = 0; m < a.inputs().size(); ++m)
				readIndices[m] = firstIteration * subiterations * a.pop(m);
			pop = readIndices.clone();
			requiredCopies.put(readIndices, (int[])pop);
		}
		if (base.type().parameterType(1).equals(int.class)) {
			assert a.outputs().size() == 1;
			push = a.push(0);
		} else {
			int[] writeIndices = new int[a.outputs().size()];
			for (int m = 0; m < a.outputs().size(); ++m)
				writeIndices[m] = firstIteration * subiterations * a.push(m);
			push = writeIndices.clone();
			requiredCopies.put(writeIndices, (int[])push);
		}
		MethodHandle loopHandle;
		if (a.worker() instanceof Filter)
			loopHandle = FILTER_LOOP;
		else if (a.worker() instanceof Splitter)
			loopHandle = SPLITTER_LOOP;
		else if (a.worker() instanceof Joiner)
			loopHandle = JOINER_LOOP;
		else
			throw new AssertionError(a);
		return MethodHandles.insertArguments(loopHandle, 0, base, unrollFactor, subiterations, pop, push);
	}

	private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
	private static final MethodHandle FILTER_LOOP = findStatic(LOOKUP, "_filterLoop");
	private static final MethodHandle SPLITTER_LOOP = findStatic(LOOKUP, "_splitterLoop");
	private static final MethodHandle JOINER_LOOP = findStatic(LOOKUP, "_joinerLoop");
	private static final MethodHandle REINITIALIZE_ARRAYS = findStatic(LOOKUP, "_reinitializeArrays");
	private static final MethodHandle OVERALL_GROUP_LOOP = findStatic(LOOKUP, "_overallGroupLoop");
	private static void _filterLoop(MethodHandle work, int iterations, int subiterations, int pop, int push, int firstIteration) throws Throwable {
		for (int i = firstIteration*subiterations; i < (firstIteration+iterations)*subiterations; ++i)
			work.invokeExact(i * pop, i * push);
	}
	private static void _splitterLoop(MethodHandle work, int iterations, int subiterations, int pop, int[] writeIndices, int firstIteration) throws Throwable {
		for (int i = firstIteration*subiterations; i < (firstIteration+iterations)*subiterations; ++i)
			work.invokeExact(i * pop, writeIndices);
	}
	private static void _joinerLoop(MethodHandle work, int iterations, int subiterations, int[] readIndices, int push, int firstIteration) throws Throwable {
		for (int i = firstIteration*subiterations; i < (firstIteration+iterations)*subiterations; ++i)
			work.invokeExact(readIndices, i * push);
	}
	private static void _reinitializeArrays(int[][] indexArrays) {
		for (int i = 0; i < indexArrays.length; i += 2)
			System.arraycopy(indexArrays[i], 0, indexArrays[i+1], 0, indexArrays[i].length);
	}
	private static void _overallGroupLoop(MethodHandle loopBody, int begin, int end, int increment) throws Throwable {
		for (int i = begin; i < end; i += increment)
			loopBody.invokeExact(i);
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

	@Override
	public String toString() {
		return "ActorGroup@"+id()+actors();
	}
}
