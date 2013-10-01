package edu.mit.streamjit.impl.compiler;

import static com.google.common.base.Preconditions.*;
import com.google.common.base.Predicate;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import edu.mit.streamjit.impl.blob.Blob.Token;
import java.util.Collections;
import java.util.List;
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
			if (a.archetype().isStateful())
				return true;
		return false;
	}

	public Set<Edge> inputs() {
		return Sets.filter(allEdges(), new Predicate<Edge>() {
			@Override
			public boolean apply(Edge input) {
				return !input.hasUpstreamActor() ||
						input.upstreamActor().group() != ActorGroup.this;
			}
		});
	}

	public Set<Edge> outputs() {
		return Sets.filter(allEdges(), new Predicate<Edge>() {
			@Override
			public boolean apply(Edge input) {
				return !input.hasDownstreamActor()||
						input.downstreamActor().group() != ActorGroup.this;
			}
		});
	}

	public Set<Edge> internalEdges() {
		return Sets.filter(allEdges(), new Predicate<Edge>() {
			@Override
			public boolean apply(Edge input) {
				return input.hasUpstreamActor() && input.hasDownstreamActor() &&
						input.upstreamActor().group() == ActorGroup.this &&
						input.downstreamActor().group() == ActorGroup.this;
			}
		});
	}

	private Set<Edge> allEdges() {
		/*
		 * Building edges is complicated by multi-edges introducing ambiguity
		 * into which edges is which (matters for rates).  We're assuming the
		 * graph is planar here.
		 */
		ImmutableSet.Builder<Edge> builder = ImmutableSet.builder();
		for (Actor a : actors) {
			Multiset<Actor> appearances = HashMultiset.create();
			for (int i = 0; i < a.predecessors().size(); ++i) {
				Object o = a.predecessors().get(i);
				int upstreamIndex;
				if (o instanceof Token)
					upstreamIndex = 0;
				else {
					/*
					 * Because the graph is planar, we know the Nth edge from
					 * one end is also the Nth edge from the other end (the
					 * multi-edges do not cross).
					 */
					Actor a2 = (Actor)o;
					upstreamIndex = findNth(a2.successors(), a, appearances.count(a2));
					appearances.add(a2);
				}
				builder.add(new Edge(o, a, upstreamIndex, i));
			}
		}

		for (Actor a : actors) {
			Multiset<Actor> appearances = HashMultiset.create();
			for (int i = 0; i < a.successors().size(); ++i) {
				Object o = a.successors().get(i);
				int downstreamIndex;
				if (o instanceof Token)
					downstreamIndex = 0;
				else {
					/*
					 * Because the graph is planar, we know the Nth edge from
					 * one end is also the Nth edge from the other end (the
					 * multi-edges do not cross).
					 */
					Actor a2 = (Actor)o;
					downstreamIndex = findNth(a2.predecessors(), a, appearances.count(a2));
					appearances.add(a2);
				}
				builder.add(new Edge(a, o, i, downstreamIndex));
			}
		}
		return builder.build();
	}

	private int findNth(List<Object> list, Object target, int n) {
		int i = 0;
		while (i != -1 && n-- >= 0) {
			list = list.subList(i+1, list.size());
			i = list.indexOf(target);
		}
		return i;
	}

	public Set<ActorGroup> predecessorGroups() {
		ImmutableSet.Builder<ActorGroup> builder = ImmutableSet.builder();
		for (Actor a : actors)
			for (Object o : a.predecessors())
				if (o instanceof Actor && ((Actor)o).group() != this)
					builder.add(((Actor)o).group());
		return builder.build();
	}

	public Set<ActorGroup> successorGroups() {
		ImmutableSet.Builder<ActorGroup> builder = ImmutableSet.builder();
		for (Actor a : actors)
			for (Object o : a.successors())
				if (o instanceof Actor && ((Actor)o).group() != this)
					builder.add(((Actor)o).group());
		return builder.build();
	}

	public ImmutableMap<Actor, Integer> schedule() {
		return schedule;
	}

	public void setSchedule(ImmutableMap<Actor, Integer> schedule) {
		for (Actor a : actors())
			checkArgument(schedule.containsKey(a), "schedule doesn't contain actor "+a);
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
