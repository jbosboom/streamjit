package edu.mit.streamjit.impl.compiler2;

import static com.google.common.base.Preconditions.*;
import edu.mit.streamjit.util.ReflectionUtils;
import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;

/**
 * The compiler IR for a Worker or Token.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 9/21/2013
 */
public abstract class Actor implements Comparable<Actor> {
	private ActorGroup group;
	/**
	 * The upstream and downstream Storage, one for each input or output of this
	 * Actor.  TokenActors will have either inputs xor outputs.
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
	protected Actor() {
	}

	public abstract int id();

	public ActorGroup group() {
		return group;
	}

	void setGroup(ActorGroup group) {
		assert ReflectionUtils.calledDirectlyFrom(ActorGroup.class);
		this.group = group;
	}

	public final boolean isPeeking() {
		for (int i = 0; i < inputs().size(); ++i)
			if (peek(i) > pop(i))
				return true;
		return false;
	}

	public abstract Class<?> inputType();

	public abstract Class<?> outputType();

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

	public abstract int peek(int input);

	public abstract int pop(int input);

	public abstract int push(int output);

	@Override
	public final int compareTo(Actor o) {
		return Integer.compare(id(), o.id());
	}

	@Override
	public final boolean equals(Object obj) {
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
	public final int hashCode() {
		return id();
	}
}
