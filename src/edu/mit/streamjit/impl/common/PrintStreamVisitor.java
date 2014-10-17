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
package edu.mit.streamjit.impl.common;

import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.StreamVisitor;
import java.io.PrintStream;
import java.io.PrintWriter;

/**
 * Prints out a structured stream graph.
 *
 * TODO: this may belong in the api package, as end-users might find it helpful
 * for debugging graph-assembly code.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 7/26/2013
 */
public final class PrintStreamVisitor extends StreamVisitor {
	private static final String INDENTATION = "    ";
	private final PrintWriter writer;
	private int indentLevel = 0;
	public PrintStreamVisitor(PrintWriter writer) {
		this.writer = writer;
	}
	public PrintStreamVisitor(PrintStream stream) {
		this(new PrintWriter(stream));
	}

	@Override
	public void beginVisit() {
	}

	@Override
	public void visitFilter(Filter<?, ?> filter) {
		indent();
		writer.println(filter.toString());
	}

	@Override
	public boolean enterPipeline(Pipeline<?, ?> pipeline) {
		indent();
		writer.println("pipeline " + pipeline.toString() + " {");
		++indentLevel;
		return true;
	}

	@Override
	public void exitPipeline(Pipeline<?, ?> pipeline) {
		--indentLevel;
		indent();
		writer.println("} //end pipeline "+pipeline);
	}

	@Override
	public boolean enterSplitjoin(Splitjoin<?, ?> splitjoin) {
		indent();
		writer.println("splitjoin "+splitjoin.toString()+" {");
		++indentLevel;
		return true;
	}

	@Override
	public void visitSplitter(Splitter<?, ?> splitter) {
		indent();
		writer.println("splitter " + splitter.toString());
	}

	@Override
	public boolean enterSplitjoinBranch(OneToOneElement<?, ?> element) {
		return true;
	}

	@Override
	public void exitSplitjoinBranch(OneToOneElement<?, ?> element) {
	}

	@Override
	public void visitJoiner(Joiner<?, ?> joiner) {
		indent();
		writer.println("joiner "+joiner.toString());
	}

	@Override
	public void exitSplitjoin(Splitjoin<?, ?> splitjoin) {
		--indentLevel;
		indent();
		writer.println("} //end splitjoin "+splitjoin);
	}

	@Override
	public void endVisit() {
		assert indentLevel == 0 : "mismatched indentation: "+indentLevel;
		writer.flush();
	}

	private void indent() {
		for (int i = 0; i < indentLevel; ++i)
			writer.write(INDENTATION);
	}
}
