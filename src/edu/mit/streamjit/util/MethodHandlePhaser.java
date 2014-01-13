package edu.mit.streamjit.util;

import com.google.common.collect.ImmutableList;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.Phaser;

/**
 * A Phaser whose onAdvance method invokes a MethodHandle.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/1/2013
 */
public final class MethodHandlePhaser extends Phaser {
	private final MethodHandle barrierAction;
	public MethodHandlePhaser(MethodHandle barrierAction, int parties) {
		super(parties);
		if (barrierAction.type().parameterCount() == 0)
			barrierAction = MethodHandles.dropArguments(barrierAction, 0, int.class, int.class);
		else if (!barrierAction.type().parameterList().equals(ImmutableList.of(int.class, int.class)))
			throw new IllegalArgumentException(barrierAction.toString());
		if (barrierAction.type().returnType().equals(void.class))
			barrierAction = MethodHandles.filterReturnValue(barrierAction,
					MethodHandles.insertArguments(MethodHandles.identity(boolean.class), 0, false));
		else if (!barrierAction.type().returnType().equals(boolean.class))
			throw new IllegalArgumentException(barrierAction.toString());
		this.barrierAction = barrierAction;
	}

	@Override
	protected final boolean onAdvance(int phase, int registeredParties) {
		try {
			return (boolean)barrierAction.invoke(phase, registeredParties);
		} catch (Throwable ex) {
			throw SneakyThrows.sneakyThrow(ex);
		}
	}
}
