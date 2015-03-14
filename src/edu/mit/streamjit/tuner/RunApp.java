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

import java.sql.ResultSet;
import java.sql.SQLException;

import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler;
import edu.mit.streamjit.impl.distributed.DistributedStreamCompiler;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmarker;

/**
 * {@link RunApp} reads configuration, streamJit's app name and location
 * information from streamjit.db based on the passed arguments, runs the
 * streamJit app and update the database with the execution time. StreamJit's
 * opentuner Python script calls this to run the streamJit application.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Sep 10, 2013
 * 
 */
public class RunApp {

	/**
	 * @param args
	 *            args[0] - program name. args[1] - tune round.
	 * @throws SQLException
	 */
	public static void main(String[] args) throws SQLException {
		if (args.length < 2)
			throw new IllegalArgumentException(
					"Not enough arguments to run the app");

		String benchmarkName = args[0];
		int round = Integer.parseInt(args[1]);

		System.out.println(String.format("JAVA Executing: %s Round - %d",
				benchmarkName, round));

		String sjDbPath = "sj" + benchmarkName + ".db";
		SqliteAdapter sjDb;
		sjDb = new SqliteAdapter();
		sjDb.connectDB(sjDbPath);
		ResultSet result = sjDb.executeQuery(String.format(
				"SELECT * FROM results WHERE Round=%d", round));

		String cfgJson = result.getString("SJConfig");
		Configuration cfg = Configuration.fromJson(cfgJson);

		Benchmark app = Benchmarker.getBenchmarkByName(benchmarkName);
		StreamCompiler sc;
		IntParameter p = cfg.getParameter("noOfMachines", IntParameter.class);
		if (p == null) {
			Compiler2StreamCompiler csc = new Compiler2StreamCompiler();
			csc.configuration(cfg);
			sc = csc;
		} else {
			sc = new DistributedStreamCompiler(p.getValue(), cfg);
		}

		double time = 0;
		try {
			Benchmarker.Result benchmarkResult = Benchmarker.runBenchmark(app,
					sc).get(0);
			if (benchmarkResult.isOK())
				time = benchmarkResult.runMillis();
			else if (benchmarkResult.kind() == Benchmarker.Result.Kind.TIMEOUT)
				time = -1;
			else if (benchmarkResult.kind() == Benchmarker.Result.Kind.EXCEPTION) {
				benchmarkResult.throwable().printStackTrace();
				time = -2;
			} else if (benchmarkResult.kind() == Benchmarker.Result.Kind.WRONG_OUTPUT) {
				System.out.println("WRONG OUTPUT");
				time = -2;
			}
		} catch (Exception e) {
			// The Benchmarker should catch everything, but just in case...
			e.printStackTrace();
			time = -2;
		} catch (OutOfMemoryError er) {
			Utils.printOutOfMemory();
			time = -3;
		}

		System.out.println(String.format("RunApp : Execution time - %f", time));

		String qry = String.format(
				"UPDATE results SET Exectime=%f WHERE Round=%d", time, round);
		sjDb.executeUpdate(qry);
	}
}
