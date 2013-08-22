package edu.mit.streamjit.test.sanity.filereaderwriterexample;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.StreamPrinter;
import edu.mit.streamjit.test.sanity.streamfilereader.StreamFileReader;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;

/**
 * Example program that shows how to use {@link StreamFileReader} filter. Run
 * {@link FileWriterExample} to generate the correct serialized file for
 * reading.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Apr 25, 2013
 */
public class FileReaderExample {

	public static void main(String[] args) throws InterruptedException {

		Input.ManualInput<Void> input = Input.createManualInput();
		Output.ManualOutput<Integer> output = Output.createManualOutput();

		StreamCompiler sc = new DebugStreamCompiler();
		CompiledStream stream = sc.compile(new filePipeline(), input, output);
		for (int i = 0; i < 10000; i++) {
			input.offer(null);
		}
		input.drain();
		while (!stream.isDrained())
			;
	}

	private static class filePipeline extends Pipeline<Void, Integer> {
		filePipeline() {
			add(new StreamFileReader<Integer>("myout.txt"));
			add(new StreamPrinter<Integer>());
		}
	}
}