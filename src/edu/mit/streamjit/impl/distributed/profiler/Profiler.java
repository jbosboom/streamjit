package edu.mit.streamjit.impl.distributed.profiler;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import edu.mit.streamjit.impl.distributed.common.Connection;

public final class Profiler extends Thread {

	/**
	 * Sampling interval in ms.
	 */
	private final int sampleInterval = 2000;

	private final Set<StreamNodeProfiler> profilers;

	private final Connection controllerConnection;

	private final AtomicBoolean stopFlag;

	public Profiler(Set<StreamNodeProfiler> profilers,
			Connection controllerConnection) {
		super("Profiler");
		this.profilers = new HashSet<>();
		checkNotNull(profilers);
		for (StreamNodeProfiler p : profilers)
			if (p != null)
				profilers.add(p);
		this.controllerConnection = checkNotNull(controllerConnection);
		stopFlag = new AtomicBoolean(false);
	}

	public void run() {
		while (true) {
			sleep();

			if (stopFlag.get())
				break;

			for (StreamNodeProfiler p : profilers) {
				try {
					controllerConnection.writeObject(p.profile());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Sleeps for t mills where (sampleInterval - 1000) < t <
	 * (sampleInterval+1000). Because, sampling based profilers must sample at
	 * random intervals. Not at fixed periods.
	 */
	private void sleep() {
		int min = (sampleInterval - 1000);
		int sleepTime = min + (int) (Math.random() * 2000);
		try {
			Thread.sleep(sleepTime);
		} catch (InterruptedException e) {
		}
	}

	public void stopProfiling() {
		stopFlag.set(true);
		this.interrupt();
	}

	public void pauseProfiling() {
	}

	public void resumeProfiling() {
	}

	public void add(StreamNodeProfiler p) {
		checkNotNull(p, "StreamNodeProfiler is null");
		profilers.add(p);
	}

	/**
	 * Removes the specified StreamNodeProfiler p from profiling.
	 * 
	 * @param p
	 *            StreamNodeProfiler that need to be removed from profiling.
	 * @return <code>true</code> iff p existed in the profiler set and has been
	 *         removed successfully.
	 */
	public boolean remove(StreamNodeProfiler p) {
		return profilers.remove(p);
	}

	/**
	 * Removes all profilers from profiling.
	 * 
	 * @param profilers
	 */
	public void removeAll(Set<StreamNodeProfiler> profilers) {
		for (StreamNodeProfiler p : profilers)
			remove(p);
	}

	/**
	 * Add all profilers for profiling.
	 * 
	 * @param profilers
	 */
	public void addAll(Set<StreamNodeProfiler> profilers) {
		for (StreamNodeProfiler p : profilers)
			add(p);
	}
}
