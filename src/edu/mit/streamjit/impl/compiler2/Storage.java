package edu.mit.streamjit.impl.compiler2;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.util.Pair;
import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
public final class Storage implements Comparable<Storage> {
	/**
	 * A persistent identifier for this Storage, based on the Actors initially
	 * connected to it.  This is not affected by removals.
	 */
	private final Token id;
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
	 * The initial data in this Storage.  The MethodHandle is a write index
	 * function that specifies where the corresponding item in the list goes.
	 * Due to these transformations, items in a later pair might precede items
	 * in an earlier pair.
	 */
	private final List<Pair<ImmutableList<Object>, MethodHandle>> initialData = new ArrayList<>();
	/**
	 * The number of data items added to and removed from this storage during
	 * each steady state iteration.
	 */
	private int throughput = -1;
	/**
	 * The span of the indices live during a steady-state iteration.
	 */
	private int steadyStateCapacity = -1;
	public Storage(Actor upstream, Actor downstream) {
		this.upstream = Lists.newArrayList(upstream);
		this.downstream = Lists.newArrayList(downstream);
		if (upstream instanceof TokenActor)
			this.id = ((TokenActor)upstream).token();
		else if (downstream instanceof TokenActor)
			this.id = ((TokenActor)downstream).token();
		else
			this.id = new Token(((WorkerActor)upstream).worker(), ((WorkerActor)downstream).worker());
	}

	public Token id() {
		return id;
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

	/**
	 * Returns true if this Storage is external; that is, more than one
	 * ActorGroup reads or writes it.  (Note that an ActorGroup may still both
	 * read and write an external storage if there is another group that reads
	 * or writes it.)
	 * @return true iff this Storage is external
	 */
	public boolean isExternal() {
		return !isInternal();
	}

	/**
	 * Returns true if this Storage is fully external; that is, all connected
	 * ActorGroups either read xor write.
	 * @return true iff this Storage is fully external
	 */
	public boolean isFullyExternal() {
		return Sets.intersection(upstreamGroups(), downstreamGroups()).isEmpty();
	}

	public List<Pair<ImmutableList<Object>, MethodHandle>> initialData() {
		return initialData;
	}

	/**
	 * Returns a set containing the indices live before the initialization
	 * schedule; that is, the indices holding initial data.  The set is
	 * recomputed on each call, so should be kept in a local variable.
	 * @return the indices holding initial data
	 */
	public ImmutableSortedSet<Integer> indicesLiveBeforeInit() {
		ImmutableSortedSet.Builder<Integer> builder = ImmutableSortedSet.naturalOrder();
		for (Pair<ImmutableList<Object>, MethodHandle> p : initialData())
			for (int i = 0; i < p.first.size(); ++i)
				try {
					builder.add((int)p.second.invokeExact(i));
				} catch (Throwable ex) {
					throw new AssertionError("index functions should not throw", ex);
				}
		return builder.build();
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
	 * Computes the type of items written into this Storage.
	 * @return the type of items written into this Storage
	 */
	public TypeToken<?> contentType() {
		Set<TypeToken<?>> types = new HashSet<>();
		for (Actor a : upstream())
			types.add(a.outputType());
		//TODO: we only really care about single wrapper types, so we don't find
		//the most specific common type etc.
		if (types.size() == 1)
			return types.iterator().next();
		return TypeToken.of(Object.class);
	}

	/**
	 * Returns the indices read from this storage during an execution of the
	 * given schedule.  The returned list is not cached so as to be responsive
	 * to changes in input index functions.
	 * @param externalSchedule the schedule
	 * @return the indices read during the given schedule under the current
	 * index functions
	 */
	public ImmutableSortedSet<Integer> readIndices(Map<ActorGroup, Integer> externalSchedule) {
		ImmutableSortedSet.Builder<Integer> builder = ImmutableSortedSet.naturalOrder();
		for (Actor a : downstream())
			builder.addAll(a.reads(this, Range.closedOpen(0, a.group().schedule().get(a) * externalSchedule.get(a.group()))));
		return builder.build();
	}

	/**
	 * Returns the indices written in this storage during an execution of the
	 * given schedule.  The returned list is not cached so as to be responsive
	 * to changes in output index functions.
	 * @param externalSchedule the schedule
	 * @return the indices written during the given schedule under the current
	 * index functions
	 */
	public ImmutableSortedSet<Integer> writeIndices(Map<ActorGroup, Integer> externalSchedule) {
		ImmutableSortedSet.Builder<Integer> builder = ImmutableSortedSet.naturalOrder();
		for (Actor a : upstream())
			builder.addAll(a.writes(this, Range.closedOpen(0, a.group().schedule().get(a) * externalSchedule.get(a.group()))));
		return builder.build();
	}

	/**
	 * Returns the number of items written to and consumed from this storage
	 * during a steady-state execution.
	 * @return the steady-state throughput
	 */
	public int throughput() {
		checkState(throughput != -1);
		return throughput;
	}

	/**
	 * Returns this Storage's steady-state capacity: the span of live elements
	 * during a steady state iteration.  This includes items to be read this
	 * iteration, items buffered for a future iteration, and space for items to
	 * be written this iteration, and possible holes in any of the above.
	 * @return this Storage's steady-state capacity
	 */
	public int steadyStateCapacity() {
		checkState(steadyStateCapacity != -1);
		return steadyStateCapacity;
	}

	/**
	 * Compute this storage's steady-state throughput and capacity.
	 * @param externalSchedule the external schedule
	 */
	public void computeSteadyStateRequirements(Map<ActorGroup, Integer> externalSchedule) {
		ImmutableSortedSet<Integer> readIndices = readIndices(externalSchedule);
		ImmutableSortedSet<Integer> writeIndices = writeIndices(externalSchedule);
		assert readIndices.isEmpty() == writeIndices.isEmpty() : readIndices+" "+writeIndices;
		this.throughput = writeIndices.size();
		if (readIndices.isEmpty())
			this.steadyStateCapacity = 0;
		else {
			int minIdx = Math.min(readIndices.first(), writeIndices.first());
			int maxIdx = Math.max(readIndices.last(), writeIndices.last());
			this.steadyStateCapacity = maxIdx - minIdx + 1;
		}
	}

	@Override
	public int compareTo(Storage o) {
		int res = id().compareTo(o.id());
		if (res == 0)
			assert equals(o) : "Can't happen! same id, different storage?";
		return res;
	}

	@Override
	public String toString() {
		return String.format("(%s, %s)", upstream, downstream);
	}
}
