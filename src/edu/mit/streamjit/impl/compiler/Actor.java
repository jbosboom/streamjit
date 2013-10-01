package edu.mit.streamjit.impl.compiler;

import com.google.common.collect.ImmutableSet;
import edu.mit.streamjit.api.Rate;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
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
	 * The upstream and downstream things: either Actors in this blob, or Tokens
	 * corresponding to Buffers at the blob boundary.
	 */
	private final List<Object> upstream = new ArrayList<>(), downstream = new ArrayList<>();
	/**
	 * Index functions (int -> int) that transform a nominal index
	 * (iteration * rate + popCount/pushCount (+ peekIndex)) into a physical
	 * index (subject to further adjustment if circular buffers are in use).
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
	public void connect(Map<Worker<?, ?>, Actor> actors) {
		List<? extends Worker<?, ?>> predecessors = Workers.getPredecessors(worker);
		if (predecessors.isEmpty())
			upstream.add(Token.createOverallInputToken(worker));
		for (Worker<?, ?> w : predecessors)
			upstream.add(actors.get(w));

		List<? extends Worker<?, ?>> successors = Workers.getSuccessors(worker);
		if (successors.isEmpty())
			downstream.add(Token.createOverallOutputToken(worker));
		for (Worker<?, ?> w : successors)
			downstream.add(actors.get(w));

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

	public List<Object> predecessors() {
		return upstream;
	}

	public List<Object> successors() {
		return downstream;
	}

	public List<MethodHandle> inputIndexFunctions() {
		return upstreamIndex;
	}

	public List<MethodHandle> outputIndexFunctions() {
		return downstreamIndex;
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
