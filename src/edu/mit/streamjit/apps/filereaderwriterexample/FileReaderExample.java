package edu.mit.streamjit.apps.filereaderwriterexample;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.StreamPrinter;
import edu.mit.streamjit.apps.streamfilereader.StreamFileReader;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;

/**
 * Example program that shows how to use {@link StreamFileReader} filter. Run {@link FileWriterExample} to generate the correct
 * serialized file for reading.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Apr 25, 2013
 */
public class FileReaderExample {

	public static void main(String[] args) throws InterruptedException {

		StreamCompiler sc = new DebugStreamCompiler();
		CompiledStream<Void, Integer> stream = sc.compile(new filePipeline());
		for (int i = 0; i < 10000; i++) {
			stream.offer(null);
		}
		stream.drain();
		while(!stream.isDrained());
	}

	private static class filePipeline extends Pipeline<Void, Integer> {
		filePipeline() {
			add(new StreamFileReader<Integer>("myout.txt"));
			add(new StreamPrinter<Integer>());
		}
	}
}