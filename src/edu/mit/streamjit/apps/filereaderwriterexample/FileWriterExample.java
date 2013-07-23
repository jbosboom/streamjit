package edu.mit.streamjit.apps.filereaderwriterexample;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.StreamFileWriter;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;

/**
 * Example program that shows how to use {@link StreamFileWriter} filter. 
 * @author Sumanan sumanan@mit.edu
 * @since Apr 29, 2013
 */
public class FileWriterExample {
	/**
	 * @param args
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException {
		StreamCompiler sc = new DebugStreamCompiler();
		CompiledStream<Double, Void> stream =  sc.compile(new StreamFileWriter<Double>("myout.txt"));
		for (Double i = 0.0; i < 1000; i++)
		{
			stream.offer(i);
		}
		stream.drain();
		while(!stream.isDrained());
	}
}