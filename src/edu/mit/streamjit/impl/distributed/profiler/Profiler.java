package edu.mit.streamjit.impl.distributed.profiler;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.impl.distributed.common.Connection;

public final class Profiler extends Thread {

	/**
	 * Sampling interval in ms.
	 */
	private final int sampleInterval = 2000;

	private final ImmutableSet<StreamNodeProfiler> profilers;

	private final Connection controllerConnection;

	private final AtomicBoolean stopFlag;

	public Profiler(ImmutableSet<StreamNodeProfiler> profilers,
			Connection controllerConnection) {
		super("Profiler");
		this.profilers = profilers;
		this.controllerConnection = controllerConnection;
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
}
