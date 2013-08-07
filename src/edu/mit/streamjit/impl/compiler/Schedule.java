package edu.mit.streamjit.impl.compiler;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/1/2013
 */
public final class Schedule<T> {
	private final ImmutableSet<T> things;
	private final ImmutableTable<T, T, Constraint<T>> constraints;
	private final ImmutableMap<T, Integer> schedule;
	private Schedule(ImmutableSet<T> things, ImmutableTable<T, T, Constraint<T>> constraints, ImmutableMap<T, Integer> schedule) {
		this.things = things;
		this.constraints = constraints;
		this.schedule = schedule;
	}
	private static <T> Schedule<T> schedule(ImmutableSet<T> things, ImmutableTable<T, T, Constraint<T>> constraints) {
		throw new UnsupportedOperationException("TODO: the actual scheduling");
	}

	public static <T> Builder<T> builder() {
		return new Builder<>();
	}

	public static final class Builder<T> {
		private final Set<T> things = new HashSet<>();
		private final Table<T, T, Constraint<T>> constraints = HashBasedTable.create();
		private Builder() {}

		public Builder<T> add(T thing) {
			things.add(checkNotNull(thing));
			return this;
		}
		public Builder<T> addAll(Iterable<T> things) {
			for (T thing : things)
				add(thing);
			return this;
		}

		public final class ConstraintBuilder {
			private final T upstream;
			private final T downstream;
			private int pushRate = -1;
			private int popRate = -1;
			private int peekRate = -1;
			private Constraint.Condition condition = null;
			private int bufferDelta = -1;
			private ConstraintBuilder(T upstream, T downstream) {
				this.upstream = upstream;
				this.downstream = downstream;
			}
			public ConstraintBuilder push(int pushRate) {
				checkArgument(pushRate > 0);
				this.pushRate = pushRate;
				return this;
			}
			public ConstraintBuilder pop(int popRate) {
				checkArgument(popRate > 0);
				this.popRate = popRate;
				return this;
			}
			public ConstraintBuilder peek(int peekRate) {
				checkArgument(peekRate >= 0);
				this.peekRate = peekRate;
				return this;
			}
			public Builder<T> bufferExactly(int items) {
				checkArgument(items >= 0);
				this.condition = Constraint.Condition.EQUAL;
				this.bufferDelta = items;
				return build();
			}
			public Builder<T> bufferAtLeast(int items) {
				checkArgument(items >= 0);
				this.condition = Constraint.Condition.GREATER_THAN_EQUAL;
				this.bufferDelta = items;
				return build();
			}
			public Builder<T> bufferAtMost(int items) {
				checkArgument(items >= 0);
				this.condition = Constraint.Condition.LESS_THAN_EQUAL;
				this.bufferDelta = items;
				return build();
			}
			public Builder<T> unconstrained() {
				return bufferAtLeast(0);
			}
			private Builder<T> build() {
				Builder.this.addConstraint(new Constraint<>(upstream, downstream, pushRate, popRate, peekRate, condition, bufferDelta));
				return Builder.this;
			}
		}

		public ConstraintBuilder connect(T upstream, T downstream) {
			checkArgument(things.contains(upstream), "upstream %s not in %s", upstream, this);
			checkArgument(things.contains(downstream), "downstream %s not in %s", downstream, this);
			checkArgument(!constraints.contains(upstream, downstream), "repeated constraint between %s and %s", upstream, downstream);
			return new ConstraintBuilder(upstream, downstream);
		}

		private void addConstraint(Constraint<T> constraint) {
			Constraint<T> old = constraints.put(constraint.upstream, constraint.downstream, constraint);
			checkArgument(old == null, "repeated constraint: %s and %s", old, constraint);
		}

		public Schedule<T> build() {
			return schedule(ImmutableSet.copyOf(things), ImmutableTable.copyOf(constraints));
		}
	}

	private static class Constraint<T> {
		private static enum Condition {
			LESS_THAN, LESS_THAN_EQUAL, EQUAL, GREATER_THAN_EQUAL, GREATER_THAN;
		};
		private final T upstream, downstream;
		private final int pushRate, popRate, excessPeeks;
		private final Condition condition;
		private final int bufferDelta;
		private Constraint(T upstream, T downstream, int pushRate, int popRate, int peekRate, Condition condition, int bufferDelta) {
			this.upstream = upstream;
			this.downstream = downstream;
			this.pushRate = pushRate;
			this.popRate = popRate;
			this.excessPeeks = Math.max(0, peekRate - popRate);
			this.condition = condition;
			this.bufferDelta = bufferDelta;
		}
	}
}
