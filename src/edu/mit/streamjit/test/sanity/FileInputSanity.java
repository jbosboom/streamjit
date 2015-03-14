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
package edu.mit.streamjit.test.sanity;

import com.google.common.base.Supplier;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.primitives.Ints;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.test.SuppliedBenchmark;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmark.Dataset;
import edu.mit.streamjit.test.BenchmarkProvider;
import edu.mit.streamjit.test.Datasets;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Set;

/**
 * Tests Input.fromBinaryFile().
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 8/23/2013
 */
@ServiceProvider(BenchmarkProvider.class)
public class FileInputSanity implements BenchmarkProvider {
	private static final Set<Integer> INPUT = ContiguousSet.create(Range.closedOpen(0, 10000), DiscreteDomain.integers());
	@Override
	public Iterator<Benchmark> iterator() {
		//TODO: generics fixes here (will change Dataset and StreamCompiler)
		Benchmark b = new SuppliedBenchmark("FileInputSanity", Identity.class,
				new Dataset("little-endian ints", Datasets.lazyInput(new WriteFileForInput(ByteOrder.LITTLE_ENDIAN)))
					.withOutput(Input.fromIterable(INPUT)),
				new Dataset("big-endian ints", Datasets.lazyInput(new WriteFileForInput(ByteOrder.BIG_ENDIAN)))
					.withOutput(Input.fromIterable(INPUT)));
		return ImmutableList.of(b).iterator();
	}

	private static final class WriteFileForInput implements Supplier<Input<Integer>> {
		private final ByteOrder byteOrder;
		private WriteFileForInput(ByteOrder byteOrder) {
			this.byteOrder = byteOrder;
		}
		@Override
		public Input<Integer> get() {
			try {
				Path path = Files.createTempFile(byteOrder.toString(), ".bin");
				ByteBuffer buffer = ByteBuffer.allocate(INPUT.size() * Ints.BYTES);

				buffer.order(byteOrder);
				IntBuffer lib = buffer.asIntBuffer();
				for (int i : INPUT)
					lib.put(i);
				try (FileChannel lc = FileChannel.open(path, StandardOpenOption.WRITE)) {
					lc.write(buffer);
					buffer.clear();
				}
				path.toFile().deleteOnExit();
				return Input.fromBinaryFile(path, Integer.class, byteOrder);
			} catch (IOException ex) {
				throw new RuntimeException(ex);
			}
		}
	}
}
