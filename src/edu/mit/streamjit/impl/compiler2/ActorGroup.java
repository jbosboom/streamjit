package edu.mit.streamjit.impl.compiler2;

import static com.google.common.base.Preconditions.*;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
		return Sets.filter(allEdges(), new Predicate<Storage>() {
			@Override
			public boolean apply(Storage input) {
				return Iterables.getOnlyElement(input.upstream()).group() != ActorGroup.this;
			}
		});
	}

	public Set<Storage> outputs() {
		return Sets.filter(allEdges(), new Predicate<Storage>() {
			@Override
			public boolean apply(Storage input) {
				return Iterables.getOnlyElement(input.downstream()).group() != ActorGroup.this;
			}
		});
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
	 * Returns a map mapping each output Storage to the set of logical indices
	 * written in that Storage during the given ActorGroup iteration.
	 * @param iteration the iteration to simulate
	 * @return a map of written logical indices
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

	@Override
	public int compareTo(ActorGroup o) {
		return Integer.compare(id(), o.id());
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final ActorGroup other = (ActorGroup)obj;
		if (id() != other.id())
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		return id();
	}
}
