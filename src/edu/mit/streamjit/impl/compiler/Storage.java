package edu.mit.streamjit.impl.compiler;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.Lists;
import edu.mit.streamjit.api.Rate;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.common.Workers;
import java.util.List;
import java.util.Objects;

/**
 * Holds information about intermediate storage in the stream graph (buffers,
 * but the name Buffer is already taken), such as the Token or Actors that read
 * and write from it.
 *
 * Rate information is only valid on an untransformed graph; Actor removal can
 * introduce ambiguity.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 9/27/2013
 */
public final class Storage {
	/**
	 * The upstream and downstream Actors or Tokens.
	 */
	private final List<Object> upstream, downstream;
	public Storage(Object upstream, Object downstream) {
		checkArgument(upstream instanceof Actor || upstream instanceof Token, upstream);
		checkArgument(downstream instanceof Actor || downstream instanceof Token, downstream);
		this.upstream = Lists.newArrayList(upstream);
		this.downstream = Lists.newArrayList(downstream);
	}

	public List<Object> upstream() {
		return upstream;
	}

	public List<Object> downstream() {
		return downstream;
	}

	public boolean hasUpstreamActor() {
		for (Object o : upstream())
			if (o instanceof Actor)
				return true;
		return false;
	}

	public Actor upstreamActor() {
		checkState(hasUpstreamActor(), this);
		checkState(upstream().size() == 1, this);
		return (Actor)upstream().get(0);
	}

	public Token upstreamToken() {
		checkState(!hasUpstreamActor(), this);
		checkState(upstream().size() == 1, this);
		return (Token)upstream().get(0);
	}

	public boolean hasDownstreamActor() {
		for (Object o : downstream())
			if (o instanceof Actor)
				return true;
		return false;
	}

	public Actor downstreamActor() {
		checkState(hasDownstreamActor(), this);
		checkState(downstream().size() == 1, this);
		return (Actor)downstream().get(0);
	}

	public Token downstreamToken() {
		checkState(!hasDownstreamActor(), this);
		checkState(downstream().size() == 1, this);
		return (Token)downstream().get(0);
	}

	public int push() {
		int upstreamIndex = hasDownstreamActor() ? Workers.getSuccessors(upstreamActor().worker()).indexOf(upstreamActor().worker()) : 0;
		Rate r = upstreamActor().worker().getPushRates().get(upstreamIndex);
		assert r.isFixed() : r;
		return r.max();
	}

	public int peek() {
		int downstreamIndex = hasUpstreamActor() ? Workers.getPredecessors(downstreamActor().worker()).indexOf(upstreamActor().worker()) : 0;
		Rate r = downstreamActor().worker().getPeekRates().get(downstreamIndex);
		assert r.isFixed() : r;
		return r.max();
	}

	public int pop() {
		int downstreamIndex = hasUpstreamActor() ? Workers.getPredecessors(downstreamActor().worker()).indexOf(upstreamActor().worker()) : 0;
		Rate r = downstreamActor().worker().getPopRates().get(downstreamIndex);
		assert r.isFixed() : r;
		return r.max();
	}

	public boolean isInternal() {
		return hasUpstreamActor() && hasDownstreamActor() &&
				upstreamActor().group() == downstreamActor().group();
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
