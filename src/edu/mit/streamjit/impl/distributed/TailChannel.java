/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.mit.streamjit.impl.distributed;

import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Stopwatch;

import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionProvider;
import edu.mit.streamjit.impl.distributed.node.TCPInputChannel;

public class TailChannel extends TCPInputChannel {

	private final int skipCount;

	private final int totalCount;

	private int count;

	private volatile CountDownLatch steadyLatch;

	private volatile CountDownLatch skipLatch;

	private performanceLogger pLogger = null;

	private boolean skipLatchUp;

	/**
	 * @param buffer
	 * @param conProvider
	 * @param conInfo
	 * @param bufferTokenName
	 * @param debugLevel
	 * @param skipCount
	 *            : Skips this amount of output before evaluating the running
	 *            time. This is added to avoid the noise from init schedule and
	 *            the drain data. ( i.e., In order to get real steady state
	 *            execution time)
	 * @param steadyCount
	 *            : {@link #getFixedOutputTime()} calculates the time taken to
	 *            get this amount of outputs ( after skipping skipCount number
	 *            of outputs at the beginning).
	 */
	public TailChannel(Buffer buffer, TCPConnectionProvider conProvider,
			TCPConnectionInfo conInfo, String bufferTokenName, int debugLevel,
			int skipCount, int steadyCount) {
		super(buffer, conProvider, conInfo, bufferTokenName, debugLevel);
		this.skipCount = skipCount;
		this.totalCount = steadyCount + skipCount;
		count = 0;
		steadyLatch = new CountDownLatch(1);
		skipLatch = new CountDownLatch(1);
		this.skipLatchUp = true;
		if (GlobalConstants.tune == 0) {
			pLogger = new performanceLogger();
			pLogger.start();
		}
	}

	@Override
	public void receiveData() {
		super.receiveData();
		count++;

		if (GlobalConstants.printOutputCount && count % 10000 == 0)
			System.err.println(count);

		if (skipLatchUp && count > skipCount) {
			skipLatch.countDown();
			skipLatchUp = false;
		}

		if (count > totalCount)
			steadyLatch.countDown();
	}

	@Override
	public void stop(int type) {
		super.stop(type);
		if (pLogger != null) {
			reset();
			pLogger.stopLogging();
		}
	}

	/**
	 * Skips skipCount amount of output at the beginning and then calculates the
	 * time taken to get steadyCount amount of outputs. skipCount is added to
	 * avoid the noise from init schedule and the drain data. ( i.e., In order
	 * to get real steady state execution time).
	 * 
	 * @return time in MILLISECONDS.
	 * @throws InterruptedException
	 */
	public long getFixedOutputTime() throws InterruptedException {
		skipLatch.await();
		Stopwatch stopwatch = Stopwatch.createStarted();
		steadyLatch.await();
		stopwatch.stop();
		return stopwatch.elapsed(TimeUnit.MILLISECONDS);
	}

	public void reset() {
		steadyLatch.countDown();
		steadyLatch = new CountDownLatch(1);
		skipLatch.countDown();
		skipLatch = new CountDownLatch(1);
		count = 0;
		skipLatchUp = true;
	}

	private class performanceLogger extends Thread {

		private AtomicBoolean stopFlag;

		private performanceLogger() {
			stopFlag = new AtomicBoolean(false);
		}

		public void run() {
			int i = 0;
			FileWriter writer;
			try {
				writer = new FileWriter("FixedOutPut.txt");
			} catch (IOException e1) {
				e1.printStackTrace();
				return;
			}
			while (++i < 10 && !stopFlag.get()) {
				try {
					Long time = getFixedOutputTime();
					System.out.println("Execution time is " + time
							+ " milli seconds");

					writer.write(time.toString());
					writer.write('\n');

					reset();

				} catch (InterruptedException | IOException e) {
					e.printStackTrace();
				}
			}
			try {
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void stopLogging() {
			stopFlag.set(true);
		}
	}
}