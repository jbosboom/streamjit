/**
 * @author Sumanan sumanan@mit.edu
 * @since Mar 7, 2013
 */

package edu.mit.streamjit.test.sanity.minimal;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.concurrent.ConcurrentStreamCompiler;
import edu.mit.streamjit.impl.distributed.DistributedStreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;

public class Minimal {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws InterruptedException {

		MinimalKernel kernel = new MinimalKernel();
//		 StreamCompiler sc = new DebugStreamCompiler();
		StreamCompiler sc = new ConcurrentStreamCompiler(2);
		// StreamCompiler sc = new DistributedStreamCompiler(2);
		CompiledStream<Integer, Void> stream = sc.compile(kernel);
		Integer output;

		// DEBUG variable
		int j = 0;
		for (int i = 0; i < 100000;) {
			if (stream.offer(i)) {
				// System.out.println("Offer success " + i);
				++i;
				j = 0;
			} else {
				j++;
				if (j > 100)
				 System.out.println("Offer failed " + i);
				Thread.sleep(10);
			}
		}

		// TODO: Analyze the need of this sleep when using the
		// DistributedStreamCompiler.
		// Thread.sleep(5000);

		stream.drain();
		System.out.println("Drain called");
		while (!stream.isDrained()) {
			Thread.sleep(100);
		}
		System.out.println("Draining finished, Exiting");
	}
}