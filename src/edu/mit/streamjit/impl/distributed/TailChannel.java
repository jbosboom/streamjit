package edu.mit.streamjit.impl.distributed;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionProvider;
import edu.mit.streamjit.impl.distributed.node.TCPInputChannel;

public class TailChannel extends TCPInputChannel {

	int limit;

	int count;

	private volatile CountDownLatch latch;

	public TailChannel(Buffer buffer, TCPConnectionProvider conProvider,
			TCPConnectionInfo conInfo, String bufferTokenName, int debugPrint,
			int limit) {
		super(buffer, conProvider, conInfo, bufferTokenName, debugPrint);
		this.limit = limit;
		count = 0;
		latch = new CountDownLatch(1);
		// new performanceLogger().start();
	}

	@Override
	public void receiveData() {
		super.receiveData();
		count++;
		// System.err.println(count);
		if (count > limit)
			latch.countDown();
	}

	public void awaitForFixInput() throws InterruptedException {
		latch.await();
	}

	public void reset() {
		latch.countDown();
		latch = new CountDownLatch(1);
		count = 0;
	}

	private class performanceLogger extends Thread {

		public void run() {
			int i = 0;
			FileWriter writer;
			try {
				writer = new FileWriter("1Interpreter.txt");
			} catch (IOException e1) {
				e1.printStackTrace();
				return;
			}
			while (++i < 30) {
				try {
					Stopwatch stopwatch = new Stopwatch();
					stopwatch.start();
					latch.await();
					stopwatch.stop();
					Long time = stopwatch.elapsed(TimeUnit.MILLISECONDS);

					// System.out.println("Execution time is " + time
					// + " milli seconds");

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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}