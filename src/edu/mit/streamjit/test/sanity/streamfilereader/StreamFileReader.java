package edu.mit.streamjit.test.sanity.streamfilereader;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.StreamFileWriter;

/**
 * This is sister class of {@link StreamFileWriter}. As this class doesn't match with current StreamJit's design, this is not included
 * into the edu.mit.streamjit.api package. Need to handle the following issues before moving this into the edu.mit.streamjit.api
 * package. 1). What to do when EOF reached? 2). This filter should support perfect Void->T generic syntax. i.e., shouldn't pop
 * anything and just read from the file and push the stream objects into the stream graph.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Apr 25, 2013
 */
public class StreamFileReader<T extends Serializable> extends Filter<Void, T> {

	ObjectInputStream in;

	public StreamFileReader(String filePath) {
		super(1, 1);
		FileInputStream fileIn;
		try {
			fileIn = new FileInputStream(filePath);
			in = new ObjectInputStream(fileIn);
		} catch (FileNotFoundException e) {
			System.out.println("Input File not found...");
			System.out.println("Terminating the executoin...");
			System.exit(1);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	@Override
	public void work() {
		pop();
		try {
			push((T) in.readObject());
		} catch (EOFException ex) {
			System.out.println("EOF file reached...");
		} catch (ClassNotFoundException | IOException ex) {
			throw new AssertionError();
		}
	}
}
