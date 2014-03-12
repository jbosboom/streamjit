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

	private final int steadyCount;

	private final int skipCount;

	private final int totalCount;

	private int count;

	private volatile CountDownLatch steadyLatch;

	private volatile CountDownLatch skipLatch;

	private performanceLogger pLogger = null;

	private boolean skipLatchUp;

	public TailChannel(Buffer buffer, TCPConnectionProvider conProvider,
			TCPConnectionInfo conInfo, String bufferTokenName, int debugLevel,
			int skipCount, int steadyCount) {
		super(buffer, conProvider, conInfo, bufferTokenName, debugLevel);
		this.steadyCount = steadyCount;
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

	public long awaitForFixInput() throws InterruptedException {
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
					Long time = awaitForFixInput();
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