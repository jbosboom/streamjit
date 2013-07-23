package edu.mit.streamjit.apps.test;

import edu.mit.streamjit.impl.blob.ConcurrentArrayBuffer;

/**
 * @author Sumanan sumanan@mit.edu
 * @since Jul 21, 2013
 */
public class TestConcurrentArrayBuffer {
	/**
	 * @param args
	 */
	public static void main(String[] args) throws InterruptedException {

		final ConcurrentArrayBuffer cab = new ConcurrentArrayBuffer(1000);
		Thread t = new Thread("ReadingThread") {
			public void run() {
				while (true)
					if (cab.size() > 0)
						System.out.println(cab.read());
			}
		};

		t.start();
		for (int i = 0; i < 100000000;) {
			if (cab.write(i))
				i++;
			else {
				// System.out.println("Writting failed " + i);
			}
		}
	}
}
