package edu.mit.streamjit.test.sanity;

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
 * Tests Input.fromBinaryFile(). This class is a BenchmarkProvider even though
 * it only uses one benchmark to have a place to put the file creation.
 * <p/>
 * TODO: we'll create the files whether or not we run the benchmark, so we need
 * something else here. We could do the initialization-on-demand-holder idiom
 * but recovering from an IOException in a static initializer seems hard/messy.
 * The cleanest way is an in-memory filesystem, but unless there's one readily
 * available...
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/23/2013
 */
@ServiceProvider(BenchmarkProvider.class)
public class FileInputSanity implements BenchmarkProvider {
	private static final Set<Integer> INPUT = ContiguousSet.create(Range.closedOpen(0, 10000), DiscreteDomain.integers());
	@Override
	public Iterator<Benchmark> iterator() {
		Path littleEndian, bigEndian;
		try {
			littleEndian = Files.createTempFile("littleEndian", ".bin");
			bigEndian = Files.createTempFile("bigEndian", ".bin");
			ByteBuffer buffer = ByteBuffer.allocate(INPUT.size() * Ints.BYTES);

			buffer.order(ByteOrder.LITTLE_ENDIAN);
			IntBuffer lib = buffer.asIntBuffer();
			for (int i : INPUT)
				lib.put(i);
			try (FileChannel lc = FileChannel.open(littleEndian, StandardOpenOption.WRITE)) {
				lc.write(buffer);
				buffer.clear();
			}

			buffer.order(ByteOrder.BIG_ENDIAN);
			IntBuffer bib = buffer.asIntBuffer();
			for (int i : INPUT)
				bib.put(i);
			try (FileChannel bc = FileChannel.open(bigEndian, StandardOpenOption.WRITE)) {
				bc.write(buffer);
				buffer.clear();
			}
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}

		//TODO: generics fixes here (will change Dataset and StreamCompiler)
		Benchmark b = new SuppliedBenchmark("FileInputSanity", Identity.class,
				Dataset.builder().name("little-endian ints")
					.input((Input)Input.fromBinaryFile(littleEndian, Integer.class, ByteOrder.LITTLE_ENDIAN))
					.output((Input)Input.fromIterable(INPUT)).build(),
				Dataset.builder().name("big-endian ints")
					.input((Input)Input.fromBinaryFile(bigEndian, Integer.class, ByteOrder.BIG_ENDIAN))
					.output((Input)Input.fromIterable(INPUT)).build());
		return ImmutableList.of(b).iterator();
	}
}
