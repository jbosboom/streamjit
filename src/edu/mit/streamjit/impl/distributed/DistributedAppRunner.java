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

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.BenchmarkProvider;
import edu.mit.streamjit.test.Benchmark.Dataset;
import edu.mit.streamjit.test.apps.channelvocoder7.ChannelVocoder7;
import edu.mit.streamjit.test.apps.fmradio.FMRadio.FMRadioBenchmarkProvider;

public class DistributedAppRunner {

	/**
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException {
		if (args.length == 0) {
			System.out.println("Usage format");
			System.out
					.println("DistributedAppRunner <BenchmarkProvider> [noOfNodes],"
							+ "[tunerMode]");

			System.out
					.println("tunerMode \n\t 0 - Start the Open tuner on a xterm. "
							+ "\n\t 1 - Open tuner will not be started automatically. "
							+ "\n\t\tUser has to start the Opentuer with the listening portNo "
							+ "argument of 12563.");
			return;
		}

		int noOfNodes = 2;
		int tunerMode = 0;

		// TODO: Need to create the BenchmarkProvider from this argument.
		String benchmarkProviderName = args[0];

		BenchmarkProvider bp = new ChannelVocoder7();

		if (args.length > 1) {
			try {
				noOfNodes = Integer.parseInt(args[1]);
			} catch (NumberFormatException ex) {
				System.err.println("Invalid noOfNodes...");
				System.err.println("Please verify the second argument.");
				System.out.println(String.format(
						"System is using default value, %d, for noofNodes.",
						noOfNodes));
			}
		}

		if (args.length > 2) {
			try {
				tunerMode = Integer.parseInt(args[2]);
			} catch (NumberFormatException ex) {
				System.err.println("Invalid tunerType...");
				System.err.println("Please verify the third argument.");
				System.out.println(String.format(
						"System is using default value, %d, for tunerType.",
						tunerMode));
			}
		}

		GlobalConstants.tunerMode = 1;

		Benchmark benchmark = bp.iterator().next();
		// StreamCompiler compiler = new Compiler2StreamCompiler();
		StreamCompiler compiler = new DistributedStreamCompiler(noOfNodes);
		// StreamCompiler compiler = new DebugStreamCompiler();
		// StreamCompiler compiler = new ConcurrentStreamCompiler(2);

		Dataset input = benchmark.inputs().get(0);
		CompiledStream stream = compiler.compile(benchmark.instantiate(),
				input.input(), Output.toPrintStream(System.out));
		stream.awaitDrained();
	}
}
