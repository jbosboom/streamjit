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

import com.google.common.base.Stopwatch;

import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
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
		if (!GlobalConstants.tune)
			new performanceLogger().start();
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
				writer = new FileWriter("FixedOutPut.txt");
			} catch (IOException e1) {
				e1.printStackTrace();
				return;
			}
			while (++i < 30) {
				try {
					Stopwatch stopwatch = Stopwatch.createStarted();
					latch.await();
					stopwatch.stop();
					Long time = stopwatch.elapsed(TimeUnit.MILLISECONDS);

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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}