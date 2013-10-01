package edu.mit.streamjit.impl.compiler;

import static com.google.common.base.Preconditions.*;
import edu.mit.streamjit.api.Rate;
import edu.mit.streamjit.impl.blob.Blob.Token;
import java.util.List;
import java.util.Objects;

/**
 * Holds information about an edge between two Actors.  (Compare IOInfo.)
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 9/27/2013
 */
public final class Edge {
	/**
	 * The upstream and downstream Actors or Tokens.
	 */
	private final Object upstream, downstream;
	private final int upstreamIndex, downstreamIndex;
	public Edge(Object upstream, Object downstream, int upstreamIndex, int downstreamIndex) {
		checkArgument(upstream instanceof Actor || upstream instanceof Token, upstream);
		checkArgument(downstream instanceof Actor || downstream instanceof Token, downstream);
		this.upstream = upstream;
		this.downstream = downstream;
		if (hasUpstreamActor()) {
			List<Object> successors = upstreamActor().successors();
			checkElementIndex(upstreamIndex, successors.size());
			checkArgument(successors.get(upstreamIndex).equals(downstream));
		} else
			checkArgument(upstreamIndex == 0);
		if (hasDownstreamActor()) {
			List<Object> predecessors = downstreamActor().predecessors();
			checkElementIndex(downstreamIndex, predecessors.size());
			checkArgument(predecessors.get(downstreamIndex).equals(upstream));
		} else
			checkArgument(downstreamIndex == 0);
		this.upstreamIndex = upstreamIndex;
		this.downstreamIndex = downstreamIndex;
	}

	public boolean hasUpstreamActor() {
		return upstream instanceof Actor;
	}

	public Actor upstreamActor() {
		checkState(hasUpstreamActor(), this);
		return (Actor)upstream;
	}

	public Token upstreamToken() {
		checkState(!hasUpstreamActor(), this);
		return (Token)upstream;
	}

	public boolean hasDownstreamActor() {
		return downstream instanceof Actor;
	}

	public Actor downstreamActor() {
		checkState(hasDownstreamActor(), this);
		return (Actor)downstream;
	}

	public Token downstreamToken() {
		checkState(!hasDownstreamActor(), this);
		return (Token)downstream;
	}

	public int push() {
		Rate r = upstreamActor().worker().getPushRates().get(upstreamIndex);
		assert r.isFixed() : r;
		return r.max();
	}

	public int peek() {
		Rate r = downstreamActor().worker().getPeekRates().get(downstreamIndex);
		assert r.isFixed() : r;
		return r.max();
	}

	public int pop() {
		Rate r = downstreamActor().worker().getPopRates().get(downstreamIndex);
		assert r.isFixed() : r;
		return r.max();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final Edge other = (Edge)obj;
		if (!Objects.equals(this.upstream, other.upstream))
			return false;
		if (!Objects.equals(this.downstream, other.downstream))
			return false;
		if (this.upstreamIndex != other.upstreamIndex)
			return false;
		if (this.downstreamIndex != other.downstreamIndex)
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		int hash = 3;
		hash = 73 * hash + Objects.hashCode(this.upstream);
		hash = 73 * hash + Objects.hashCode(this.downstream);
		hash = 73 * hash + this.upstreamIndex;
		hash = 73 * hash + this.downstreamIndex;
		return hash;
	}

	@Override
	public String toString() {
		return String.format("(%s, %s)", upstream, downstream);
	}
}
