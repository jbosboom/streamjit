package edu.mit.streamjit.impl.compiler2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.DrainData;
import java.lang.invoke.MethodHandle;
import java.util.Map;
import java.util.Set;

/**
 * The actual blob produced by a Compiler2.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/1/2013
 */
public class Compiler2BlobHost implements Blob {
	private final ImmutableSet<Worker<?, ?>> workers;
	private final ImmutableSortedSet<Token> inputTokens, outputTokens;
	private final MethodHandle initCode;
	private final ImmutableList<MethodHandle> steadyStateCode;
	private final ImmutableMap<Token, Integer> tokenInitSchedule, tokenSteadyStateSchedule;
	@Override
	public Set<Worker<?, ?>> getWorkers() {
		return workers;
	}

	@Override
	public Set<Token> getInputs() {
		return inputTokens;
	}

	@Override
	public Set<Token> getOutputs() {
		return outputTokens;
	}

	@Override
	public int getMinimumBufferCapacity(Token token) {
		if (inputTokens.contains(token))
			return Math.max(tokenInitSchedule.get(token), tokenSteadyStateSchedule.get(token));
		if (outputTokens.contains(token))
			return 1;
		throw new IllegalArgumentException(token.toString()+" not an input or output of this blob");
	}

	@Override
	public void installBuffers(Map<Token, Buffer> buffers) {
		throw new UnsupportedOperationException("TODO");
	}

	@Override
	public int getCoreCount() {
		return steadyStateCode.size();
	}

	@Override
	public Runnable getCoreCode(int core) {
		throw new UnsupportedOperationException("TODO");
	}

	@Override
	public void drain(Runnable callback) {
		throw new UnsupportedOperationException("TODO");
	}

	@Override
	public DrainData getDrainData() {
		throw new UnsupportedOperationException("TODO");
	}
}
