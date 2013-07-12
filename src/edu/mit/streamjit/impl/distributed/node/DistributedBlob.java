package edu.mit.streamjit.impl.distributed.node;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.common.IOInfo;
import edu.mit.streamjit.impl.common.MessageConstraint;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.concurrent.SingleThreadedBlob;
import edu.mit.streamjit.impl.interp.Channel;
import edu.mit.streamjit.impl.interp.Interpreter;

/**
 * {@link DistributedBlob} runs set of workers in multiple threads. Caller must provide the thread level partition of the workers.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */

public class DistributedBlob implements Blob {

	private List<Blob> blobList;

	/**
	 * Absolute input and output {@link Channel}s of a {@link Blob}
	 */
	private final ImmutableMap<Token, Channel<?>> inputChannels, outputChannels;

	/**
	 * @param workersIter
	 *            : {@link Iterable} of {@link Iterable} of workers. First {@link Iterable} is thread level assignment. Each first
	 *            level {@link Iterable}s will be passed to {@link SingleThreadedBlob} and will be running in a single thread.
	 * @param constraintsIter
	 */
	public DistributedBlob(List<Set<Worker<?, ?>>> workersIter, Iterable<MessageConstraint> constraintsIter, boolean isControllerNode) {
		blobList = new ArrayList<>();

		for (Iterable<Worker<?, ?>> coreWorkers : workersIter) {
			blobList.add(new SingleThreadedBlob(coreWorkers, constraintsIter));
		}

		ImmutableMap.Builder<Token, Channel<?>> inputChannelsBuilder = ImmutableMap.builder();
		ImmutableMap.Builder<Token, Channel<?>> outputChannelsBuilder = ImmutableMap.builder();
		for (IOInfo info : IOInfo.create(getWorkers())) {
			if (info.isInput() && info.upstream() == null && isControllerNode)
				continue;
			if (info.isOutput() && info.downstream() == null && isControllerNode)
				continue;
			(info.isInput() ? inputChannelsBuilder : outputChannelsBuilder).put(info.token(), info.channel());
		}
		this.inputChannels = inputChannelsBuilder.build();
		this.outputChannels = outputChannelsBuilder.build();
	}

	@Override
	public Set<Worker<?, ?>> getWorkers() {
		Set<Worker<?, ?>> workers = new HashSet<>();

		for (Blob b : blobList) {
			workers.addAll(b.getWorkers());
		}
		return workers;
	}

	@Override
	public Map<Token, Channel<?>> getInputChannels() {
		return inputChannels;
	}

	@Override
	public Map<Token, Channel<?>> getOutputChannels() {
		return outputChannels;
	}

	@Override
	public int getCoreCount() {
		return blobList.size();
	}

	@Override
	public Runnable getCoreCode(int core) {
		if (core >= blobList.size())
			throw new IllegalArgumentException(String.format(
					"Total cores assigned to this blob is %d, but Core code for the core %d has been asked.", blobList.size(), core));
		return blobList.get(core).getCoreCode(0);
	}

	@Override
	public void drain(Runnable callback) {

	}
}
