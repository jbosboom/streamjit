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
package edu.mit.streamjit.test.regression;

import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmarker;
import edu.mit.streamjit.test.Datasets;
import java.util.Collections;
import java.util.List;

/**
 * @since 11/26/2013 10:54PM EST
 */
@ServiceProvider(Benchmark.class)
public class Reg20131126_105423_908 implements Benchmark {
	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public OneToOneElement<Object, Object> instantiate() {
		return
//		new Pipeline(new OneToOneElement[]{
//			new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
				new Pipeline(new OneToOneElement[]{
//					new edu.mit.streamjit.impl.common.TestFilters.Batcher(2),
//					new Pipeline(new OneToOneElement[]{
						new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(2), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
							new edu.mit.streamjit.impl.common.TestFilters.PeekingAdder(10),
							new edu.mit.streamjit.impl.common.TestFilters.Adder(20),
//							new edu.mit.streamjit.impl.common.TestFilters.Adder(20)
						}),
//						new edu.mit.streamjit.impl.common.TestFilters.Adder(1),
//						new edu.mit.streamjit.impl.common.TestFilters.Adder(20),
//						new edu.mit.streamjit.impl.common.TestFilters.Multiplier(100)
//					}),
					new edu.mit.streamjit.impl.common.TestFilters.PeekingAdder(10)
				});
//			})
//		});
	}
	@Override
	public List<Dataset> inputs() {
		Dataset ds = Datasets.allIntsInRange(0, 1000);
		return Collections.singletonList(ds.withOutput(Datasets.outputOf(new edu.mit.streamjit.impl.interp.InterpreterStreamCompiler(), instantiate(), ds.input())));
	}
	@Override
	public String toString() {
		return getClass().getSimpleName();
	}
	public static void main(String[] args) {
		Benchmarker.runBenchmark(new Reg20131126_105423_908(), new edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler()).get(0).print(System.out);
	}
}

