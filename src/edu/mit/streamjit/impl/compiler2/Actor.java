package edu.mit.streamjit.impl.compiler2;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Table;
import edu.mit.streamjit.api.Rate;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.util.ReflectionUtils;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The compiler IR node for a single worker.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 9/21/2013
 */
public class Actor implements Comparable<Actor> {
	private final Worker<?, ?> worker;
	private final ActorArchetype archetype;
	private ActorGroup group;
	/**
	 * The upstream and downstream Storage, one for each input or output of this
	 * Actor.
	 */
	private final List<Storage> upstream = new ArrayList<>(), downstream = new ArrayList<>();
	/**
	 * Index functions (int -> int) that transform a nominal index
	 * (iteration * rate + popCount/pushCount (+ peekIndex)) into a physical
	 * index (subject to further adjustment if circular buffers are in use).
	 * One for each input or output of this actor.
	 */
	private final List<MethodHandle> upstreamIndex = new ArrayList<>(),
			downstreamIndex = new ArrayList<>();
	public Actor(Worker<?, ?> worker, ActorArchetype archetype) {
		this.worker = worker;
		this.archetype = archetype;
	}

	/**
	 * Sets up Actor connections based on the worker's predecessor/successor
	 * relationships.
	 */
	public void connect(Map<Worker<?, ?>, Actor> actors, Table<Object, Object, Storage> storage) {
		List<? extends Worker<?, ?>> predecessors = Workers.getPredecessors(worker);
		if (predecessors.isEmpty()) {
			Token t = Token.createOverallInputToken(worker);
			Storage s = new Storage(t, this);
			upstream.add(s);
			storage.put(t, this, s);
		}
		for (Worker<?, ?> w : predecessors) {
			Object pred = actors.get(w);
			if (pred == null)
				pred = new Token(w, worker());
			Storage s = new Storage(pred, this);
			upstream.add(s);
			storage.put(pred, this, s);
		}

		List<? extends Worker<?, ?>> successors = Workers.getSuccessors(worker);
		if (successors.isEmpty()) {
			Token t = Token.createOverallOutputToken(worker);
			Storage s = new Storage(this, t);
			downstream.add(s);
			storage.put(this, t, s);
		}
		for (Worker<?, ?> w : successors) {
			Object succ = actors.get(w);
			if (succ == null)
				succ = new Token(worker(), w);
			Storage s = new Storage(this, succ);
			downstream.add(s);
			storage.put(succ, this, s);
		}

		MethodHandle identity = MethodHandles.identity(int.class);
		upstreamIndex.addAll(Collections.nCopies(upstream.size(), identity));
		downstreamIndex.addAll(Collections.nCopies(downstream.size(), identity));
	}

	public int id() {
		return Workers.getIdentifier(worker());
	}

	public Worker<?, ?> worker() {
		return worker;
	}

	public ActorArchetype archetype() {
		return archetype;
	}

	public ActorGroup group() {
		return group;
	}

	void setGroup(ActorGroup group) {
		assert ReflectionUtils.calledDirectlyFrom(ActorGroup.class);
		this.group = group;
	}

	public boolean isPeeking() {
		List<Rate> peeks = worker.getPeekRates(), pops = worker.getPopRates();
		assert peeks.size() == pops.size();
		for (int i = 0; i < peeks.size(); ++i)
			if (peeks.get(i).max() == Rate.DYNAMIC || peeks.get(i).max() > pops.get(i).max())
				return true;
		return false;
	}

	public List<Storage> inputs() {
		return upstream;
	}

	public List<Storage> outputs() {
		return downstream;
	}

	public List<MethodHandle> inputIndexFunctions() {
		return upstreamIndex;
	}

	public int translateInputIndex(int input, int logicalIndex) {
		checkArgument(logicalIndex >= 0);
		try {
			return (int)inputIndexFunctions().get(input).invokeExact(logicalIndex);
		} catch (Throwable ex) {
			throw new AssertionError(String.format("index functions should not throw; translateInputIndex(%d, %d)", input, logicalIndex), ex);
		}
	}

	public List<MethodHandle> outputIndexFunctions() {
		return downstreamIndex;
	}

	public int translateOutputIndex(int output, int logicalIndex) {
		checkArgument(logicalIndex >= 0);
		try {
			return (int)outputIndexFunctions().get(output).invokeExact(logicalIndex);
		} catch (Throwable ex) {
			throw new AssertionError(String.format("index functions should not throw; translateOutputtIndex(%d, %d)", output, logicalIndex), ex);
		}
	}

	@Override
	public int compareTo(Actor o) {
		return Integer.compare(id(), o.id());
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final Actor other = (Actor)obj;
		if (id() != other.id())
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		return id();
	}

	public static ImmutableSet<Worker<?, ?>> unwrap(Set<Actor> actors) {
		ImmutableSet.Builder<Worker<?, ?>> builder = ImmutableSet.builder();
		for (Actor a : actors)
			builder.add(a.worker());
		return builder.build();
	}
}
