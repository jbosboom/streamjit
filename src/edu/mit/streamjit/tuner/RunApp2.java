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
package edu.mit.streamjit.tuner;

import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmarker;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * Runs a benchmark using a specified configuration.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/21/2014
 */
public final class RunApp2 {
	private RunApp2() {}

	public static void main(String[] args) throws IOException {
		//If the configuration argument starts with @, it names a file holding
		//the configuration (to work around command line max length).
		Configuration cfg;
		if (args[0].startsWith("@")) {
			List<String> lines = Files.readAllLines(Paths.get(args[0].substring(1)), StandardCharsets.UTF_8);
//			if (lines.size() != 1)
//				throw new RuntimeException("Response file contained "+lines.size()+" lines");
			cfg = Configuration.fromJson(lines.get(0));
		} else
			cfg = Configuration.fromJson(args[0]);
		Benchmark bm = Benchmarker.getBenchmarkByName((String)cfg.getExtraData("benchmark"));
		//TODO: we should be passing the StreamCompiler somewhere (also extra data?)
		StreamCompiler sc = new Compiler2StreamCompiler().configuration(cfg).reportThroughput();

		long time = -1;
		try {
			Benchmarker.Result benchmarkResult = Benchmarker.runBenchmark(bm, sc).get(0);
			if (benchmarkResult.isOK())
				time = benchmarkResult.runMillis();
			else if (benchmarkResult.kind() == Benchmarker.Result.Kind.TIMEOUT) {
				System.err.println("TIMED OUT");
				time = -1;
			} else if (benchmarkResult.kind() == Benchmarker.Result.Kind.EXCEPTION) {
				benchmarkResult.throwable().printStackTrace();
				time = -1;
			} else if (benchmarkResult.kind() == Benchmarker.Result.Kind.WRONG_OUTPUT) {
				System.err.println("WRONG OUTPUT");
				time = -1;
			}
		} catch (Throwable e) {
			//The Benchmarker should catch everything, but just in case...
			e.printStackTrace();
			time = -1;
		}
		System.out.println(time);
		//If we time out, threads may prevent termination.  Force it.
		System.exit(0);
	}
}
