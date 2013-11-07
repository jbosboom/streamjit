package edu.mit.streamjit.impl.compiler2;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Holds information about intermediate storage in the stream graph (buffers,
 * but the name Buffer is already taken), such as the Actors that read
 * and write from it.
 *
 * Rate information is only valid on an untransformed graph; Actor removal can
 * introduce ambiguity.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 9/27/2013
 */
public final class Storage {
	/**
	 * The upstream and downstream Actors.
	 */
	private final List<Actor> upstream, downstream;
	/**
	 * The type of data stored in this storage.  Initially this is Object, but
	 * unboxing may change it to a primitive type after examining the connected
	 * Actors.
	 */
	private Class<?> type = Object.class;
	/**
	 * The number of data items added to and removed from this storage during
	 * each steady state iteration.
	 */
	private int throughput = -1;
	/**
	 * The logical indices in this storage read by outputs in a steady-state
	 * iteration. These values must be live at the beginning of the steady-state
	 * iteration, and thus determine the minimum buffering requirement.
	 */
	private ImmutableSortedSet<Integer> readIndices;
	/**
	 * The max number of elements live in this storage during initialization.
	 */
	private int initCapacity = -1;
	/**
	 * The indices live in this storage after the initialization schedule is
	 * complete; that is, the data that must be migrated.
	 */
	private ImmutableSortedSet<Integer> liveAfterInit;
	/**
	 * The indices live in this storage at the beginning of a steady-state
	 * execution.
	 */
	private ImmutableSortedSet<Integer> liveDuringSteadyState;
	public Storage(Actor upstream, Actor downstream) {
		this.upstream = Lists.newArrayList(upstream);
		this.downstream = Lists.newArrayList(downstream);
	}

	public List<Actor> upstream() {
		return upstream;
	}

	public ImmutableSet<ActorGroup> upstreamGroups() {
		ImmutableSet.Builder<ActorGroup> builder = ImmutableSet.builder();
		for (Actor a : upstream())
			builder.add(a.group());
		return builder.build();
	}

	public List<Actor> downstream() {
		return downstream;
	}

	public ImmutableSet<ActorGroup> downstreamGroups() {
		ImmutableSet.Builder<ActorGroup> builder = ImmutableSet.builder();
		for (Actor a : downstream())
			builder.add(a.group());
		return builder.build();
	}

	public int push() {
		checkState(upstream().size() == 1, this);
		return upstream().get(0).push(upstream().get(0).outputs().indexOf(this));
	}

	public int peek() {
		checkState(downstream().size() == 1, this);
		return downstream().get(0).peek(downstream().get(0).inputs().indexOf(this));
	}

	public int pop() {
		checkState(downstream().size() == 1, this);
		return downstream().get(0).pop(downstream().get(0).inputs().indexOf(this));
	}

	/**
	 * Returns true if this Storage is internal to an ActorGroup; that is, all
	 * Actors reading or writing it are in the same ActorGroup.
	 * @return true iff this Storage is internal to an ActorGroup
	 */
	public boolean isInternal() {
		ActorGroup g = upstream().get(0).group();
		for (Actor a : upstream())
			if (a.group() != g)
				return false;
		for (Actor a : downstream())
			if (a.group() != g)
				return false;
		return true;
	}

	public Class<?> type() {
		return type;
	}

	public void setType(Class<?> type) {
		//We could check the new type is compatible with the common type if we
		//consider primitives compatible with their wrapper type.
		this.type = type;
	}

	/**
	 * Computes the common type of the Actors connected to this Storage.
	 * @return the common type of the Actors connected to this Storage
	 */
	public Class<?> commonType() {
		Set<Class<?>> types = new HashSet<>();
		for (Actor a : upstream())
			types.add(a.outputType());
		for (Actor a : downstream())
			types.add(a.inputType());
		//TODO: we only really care about the case where the common types are
		//all one (wrapper) type, so check that and return Object otherwise.
		if (types.size() == 1)
			return types.iterator().next();
		return Object.class;
	}

	public int throughput() {
		checkState(throughput != -1);
		return throughput;
	}

	public ImmutableSortedSet<Integer> readIndices() {
		checkState(readIndices != null);
		return readIndices;
	}

	/**
	 * Returns this Storage's steady-state capacity: the span of live elements
	 * during a steady state iteration.  This includes items to be read this
	 * iteration, items buffered for a future iteration, and space for items to
	 * be written this iteration, and possible holes in any of the above.
	 * @return this Storage's steady-state capacity
	 */
	public int steadyStateCapacity() {
		if (isInternal())
			return throughput();
		//The indices live at the beginning of the iteration, plus space for
		//items being written.  (Some writes are into new holes, but others
		//create new holes, so it balances out.)
		int span = indicesLiveDuringSteadyState().last() - indicesLiveDuringSteadyState().first() + 1;
		return span + throughput();
	}

	/**
	 * Compute this storage's size requirements based on the index functions.
	 * (This is not the actual buffer capacity, because the init schedule might
	 * require additional buffering to meet some other storage's requirement.)
	 * TODO: perhaps use ActorGroup.reads()/writes() in here?  Would duplicate
	 * work because we only care about one Storage here, but would simplify code.
	 * Or maybe we should move this work to Compiler2.initSchedule() so we can
	 * compute with all Storages at once?
	 * @param externalSchedule the external schedule
	 */
	public void computeRequirements(Map<ActorGroup, Integer> externalSchedule) {
		/*
		 * To compute the throughput, we just count the elements written; they
		 * should be both dense and non-overlapping.  TODO: if we intrisify a
		 * decimator by making some writes no-ops, this may not hold.
		 */
		throughput = 0;
		for (Actor a : upstream()) {
			int executions = (isInternal() ? 1 : externalSchedule.get(a.group())) * a.group().schedule().get(a);
			for (int i = 0; i < a.outputs().size(); ++i)
				if (a.outputs().get(i).equals(this))
					throughput += a.push(i) * executions;
		}

		/**
		 * Now find the indices that could be read during a steady state
		 * execution. That's the initialization requirement.
		 */
		ImmutableSortedSet.Builder<Integer> readIndicesBuilder = ImmutableSortedSet.naturalOrder();
		for (Actor a : downstream()) {
			int executions = a.group().schedule().get(a) * (isInternal() ? 1 : externalSchedule.get(a.group()));
			for (int i = 0; i < a.inputs().size(); ++i) {
				if (!a.inputs().get(i).equals(this)) continue;
				int pop = a.pop(i),	peek = a.peek(i);
				int excessPeeks = Math.max(0, peek - pop);
				int maxLogicalIndex = pop * executions + excessPeeks;
				for (int idx = 0; idx < maxLogicalIndex; ++idx)
					readIndicesBuilder.add(a.translateInputIndex(i, idx));
			}
		}
		this.readIndices = readIndicesBuilder.build();
	}

	public int initCapacity() {
		checkState(initCapacity != -1);
		return initCapacity;
	}

	public void setInitCapacity(int initCapacity) {
		this.initCapacity = initCapacity;
	}

	public ImmutableSortedSet<Integer> indicesLiveAfterInit() {
		checkState(liveAfterInit != null);
		return liveAfterInit;
	}

	public void setIndicesLiveAfterInit(ImmutableSortedSet<Integer> liveAfterInit) {
		this.liveAfterInit = liveAfterInit;
		if (this.liveAfterInit.isEmpty())
			this.liveDuringSteadyState = ImmutableSortedSet.of();
		else {
			int offset = liveAfterInit.first() - readIndices().first();
			ImmutableSortedSet.Builder<Integer> b = ImmutableSortedSet.naturalOrder();
			for (Integer i : this.liveAfterInit)
				b.add(i-offset);
			this.liveDuringSteadyState = b.build();
		}
	}

	public ImmutableSortedSet<Integer> indicesLiveDuringSteadyState() {
		checkState(liveDuringSteadyState != null);
		return liveDuringSteadyState;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final Storage other = (Storage)obj;
		if (!Objects.equals(this.upstream, other.upstream))
			return false;
		if (!Objects.equals(this.downstream, other.downstream))
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		int hash = 3;
		hash = 73 * hash + Objects.hashCode(this.upstream);
		hash = 73 * hash + Objects.hashCode(this.downstream);
		return hash;
	}

	@Override
	public String toString() {
		return String.format("(%s, %s)", upstream, downstream);
	}
}
