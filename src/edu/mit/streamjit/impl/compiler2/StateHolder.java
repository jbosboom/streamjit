package edu.mit.streamjit.impl.compiler2;

import com.google.common.collect.ImmutableMap;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.compiler2.Compiler2BlobHost.DrainInstruction;
import edu.mit.streamjit.util.ReflectionUtils;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;

/**
 * Subclasses of this class hold worker state.
 * TODO: base iteration count as pseudo-state
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 12/11/2013
 */
public abstract class StateHolder implements DrainInstruction {
	private final Worker<?, ?> worker;
	protected StateHolder(Worker<?, ?> worker) {
		this.worker = worker;
	}

	/**
	 * Rather than produce data, "drain" by moving any non-final fields back
	 * into the worker.  (They'll then be used by the interpreter during
	 * draining and eventually collected into a DrainData.)
	 * @return an empty immutable map
	 */
	@Override
	public Map<Blob.Token, Object[]> call() {
		for (Field hf : getClass().getDeclaredFields()) {
			Field wf = ReflectionUtils.getFieldByName(worker, hf.getName());
			if (!Modifier.isFinal(wf.getModifiers()))
				try {
					wf.setAccessible(true);
					wf.set(worker, hf.get(this));
				} catch (IllegalAccessException ex) {
					throw new AssertionError(ex);
				}
		}
		return ImmutableMap.of();
	}
}
