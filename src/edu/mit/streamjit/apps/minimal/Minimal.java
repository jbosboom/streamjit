/**
 * @author Sumanan sumanan@mit.edu
 * @since Mar 7, 2013
 */

package edu.mit.streamjit.apps.minimal;

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
		// StreamCompiler sc = new DebugStreamCompiler();
		// StreamCompiler sc = new ConcurrentStreamCompiler(4);
		StreamCompiler sc = new DistributedStreamCompiler(2);
		CompiledStream<Integer, Void> stream = sc.compile(kernel);
		Integer output;
		for (int i = 0; i < 1000000; ++i) {
			stream.offer(i);
			// System.out.println("Offered" + i);
			// while ((output = stream.poll()) != null)
			// System.out.println(output);
		}
		// TODO: Analyze the need of this sleep when using the DistributedStreamCompiler. 
		Thread.sleep(5000);
		
		stream.drain();
		System.out.println("Drain called");
		stream.awaitDraining();
		System.out.println("awaitDraining finished, Exiting");
		System.out.println("Main is exiting...");
	}
}