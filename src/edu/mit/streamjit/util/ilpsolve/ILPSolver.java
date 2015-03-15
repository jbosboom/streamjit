/*
 * Copyright (c) 2013-2015 Massachusetts Institute of Technology
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
package edu.mit.streamjit.util.ilpsolve;

import edu.mit.streamjit.util.ilpsolve.lib.Bindings;
import static edu.mit.streamjit.util.ilpsolve.lib.Bindings.*;
import org.bridj.Pointer;
import static com.google.common.base.Preconditions.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.math.DoubleMath;
import com.google.common.math.IntMath;
import java.io.IOException;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 8/5/2013
 */
public final class ILPSolver {
	private final List<Variable> variables = new ArrayList<>();
	private final List<Constraint> constraints = new ArrayList<>();
	private ObjectiveFunction objFn = null;
	private boolean solved = false;
	public ILPSolver() {}

	public Variable newVariable() {
		return newVariable(null);
	}
	public Variable newVariable(String name) {
		checkState(!solved, "system already solved");
		Variable variable = new Variable(name);
		variables.add(variable);
		return variable;
	}

	public LinearExpr newLinearExpr(Map<Variable, Integer> coefficients) {
		checkArgument(!coefficients.isEmpty());
		checkArgument(!coefficients.values().contains(0));
		for (Variable v : coefficients.keySet())
			checkArgument(v.enclosingInstance() == this);
		return new LinearExpr(coefficients);
	}

	public Constraint constrainEquals(LinearExpr expr, int value) {
		return constrain(expr, EQ, value);
	}
	public Constraint constrainAtLeast(LinearExpr expr, int value) {
		return constrain(expr, GE, value);
	}
	public Constraint constrainAtMost(LinearExpr expr, int value) {
		return constrain(expr, LE, value);
	}
	private Constraint constrain(LinearExpr expr, int signum, int value) {
		checkState(!solved, "system already solved");
		Constraint constraint = new Constraint(expr, signum, value);
		constraints.add(constraint);
		return constraint;
	}

	public ObjectiveFunction maximize(LinearExpr expr) {
		return optimize(expr, 1);
	}
	public ObjectiveFunction minimize(LinearExpr expr) {
		return optimize(expr, -1);
	}
	private ObjectiveFunction optimize(LinearExpr expr, int signum) {
		checkState(!solved, "system already solved");
		checkState(objFn == null, "objective function already set");
		objFn = new ObjectiveFunction(expr, signum);
		return objFn;
	}

	public void solve() {
		checkState(!solved, "system already solved");
		checkState(objFn != null, "objective function not set");

		Pointer<lprec> lp = null;
		Pointer<Double> row = null, column = null;
		Path logFile = null;
		try {
			//Row 0 is the objective function.  Col 0 is apparently the right-hand
			//side but the API hides this.
			lp = makeLp(constraints.size(), variables.size());
			row = Pointer.allocateDoubles(variables.size()+1);
			column = Pointer.allocateDoubles(constraints.size()+1);
			if (lp == null || row == null || column == null)
				throw new OutOfMemoryError();


			boolean assertionsEnabled = false;
			assert assertionsEnabled = true; //Intentional side effect.
			if (assertionsEnabled) {
				try {
					logFile = Files.createTempFile("lp", null);
				} catch (IOException ignored) {}
				Pointer<Byte> emptyString = Pointer.pointerToCString(logFile != null ? logFile.toString() : "");
				setOutputfile(lp, emptyString);
				emptyString.release();
				setVerbose(lp, FULL);
			} else {
				setVerbose(lp, IMPORTANT);
			}

			setAddRowmode(lp, (byte)1);
			storeCoefficientsInRow(objFn.expr, row);
			setObjFn(lp, row);

			for (int i = 0; i < constraints.size(); ++i) {
				Constraint c = constraints.get(i);
				storeCoefficientsInRow(c.expr, row);
				setRow(lp, i+1, row);
			}
			setAddRowmode(lp, (byte)0);

			if (objFn.signum == 1)
				setMaxim(lp);
			else if (objFn.signum == -1)
				setMinim(lp);
			else throw new AssertionError(objFn.signum);

			for (int i = 0; i < variables.size(); ++i) {
				Variable v = variables.get(i);
				Pointer<Byte> name = Pointer.pointerToCString(v.name());
				setColName(lp, i+1, name);
				name.release();

				setBounds(lp, i+1, v.lowerBound, v.upperBound);
				setInt(lp, i+1, (byte)1);
			}

			for (int i = 0; i < constraints.size(); ++i) {
				Constraint c = constraints.get(i);
				setConstrType(lp, i+1, c.signum);
				column.setDoubleAtIndex(i+1, c.value);
			}
			setRhVec(lp, column);

			int solret = Bindings.solve(lp);
			if (solret == OPTIMAL) {
				//For some reason get_variables is 0-based unlike the rest of the API.
				getVariables(lp, row);
				for (int i = 0; i < variables.size(); ++i)
					variables.get(i).value = DoubleMath.roundToInt(row.getDoubleAtIndex(i), RoundingMode.UNNECESSARY);
				solved = true;
			} else {
				printLp(lp);
				String log = "";
				if (logFile != null) {
					try {
						List<String> logLines = Files.readAllLines(logFile, StandardCharsets.UTF_8);
						StringBuilder sb = new StringBuilder("\n");
						for (String s : logLines)
							sb.append(s).append("\n");
						log = sb.toString();
					} catch (IOException ignored) {}
				}

				switch (solret) {
					case NOMEMORY:
						throw new OutOfMemoryError("when solving"+log);
					case SUBOPTIMAL:
						throw new AssertionError("suboptimal; shouldn't happen?"+log);
					case INFEASIBLE:
						throw new InfeasibleSystemException(log);
					case UNBOUNDED:
						throw new SolverException("system is unbounded"+log);
					case DEGENERATE:
						throw new SolverException("system is degenerate"+log);
					case NUMFAILURE:
						throw new SolverException("numerical failure"+log);
					case USERABORT:
						throw new AssertionError("can't happen; user abort not used"+log);
					case TIMEOUT:
						throw new AssertionError("can't happen; timeout not used"+log);
					case PRESOLVED:
						throw new AssertionError("can't happen; presolve not used"+log);
					case PROCFAIL:
						throw new SolverException("B&B failure"+log);
					case PROCBREAK:
						throw new AssertionError("can't happen; B&B early breaks not used"+log);
					case FEASFOUND:
						throw new AssertionError("feasfound"+log);
					case NOFEASFOUND:
						throw new AssertionError("nofeasfound"+log);
				}
			}
		} finally {
			if (column != null)
				column.release();
			if (row != null)
				row.release();
			if (lp != null)
				deleteLp(lp);
			if (logFile != null)
				try {
					Files.delete(logFile);
				} catch (IOException ignored) {}
		}
	}

	private void storeCoefficientsInRow(LinearExpr expr, Pointer<Double> row) {
		for (int i = 0; i < variables.size(); ++i) {
			Integer c = expr.coefficients.get(variables.get(i));
			row.setDoubleAtIndex(i+1, c != null ? c : 0);
		}
	}

	/**
	 * Represents a variable to be optimized.
	 *
	 * Note that while this class has a name, it is for debugging purposes only.
	 * Variables use reference equality.
	 */
	public final class Variable {
		private final String name;
		private int lowerBound = 0, upperBound = Integer.MAX_VALUE;
		private int value;
		public Variable(String name) {
			this.name = name;
		}
		/**
		 * Returns the name of this variable.  May be null.
		 * @return the name of this variable (may be null)
		 */
		public String name() {
			return name;
		}

		public Variable lowerBound(int lowerBound) {
			this.lowerBound = lowerBound;
			return this;
		}
		public Variable upperBound(int upperBound) {
			this.upperBound = upperBound;
			return this;
		}

		public LinearExpr asLinearExpr(int coefficient) {
			return new LinearExpr(ImmutableMap.of(this, coefficient));
		}

		public int value() {
			checkState(solved, "system not yet solved");
			return value;
		}

		/* package-private */ ILPSolver enclosingInstance() {
			return ILPSolver.this;
		}
	}

	public final class LinearExpr {
		private ImmutableMap<Variable, Integer> coefficients;
		private LinearExpr(Map<Variable, Integer> coefficients) {
			assert !coefficients.isEmpty();
			this.coefficients = ImmutableMap.copyOf(coefficients);
		}

		public LinearExpr plus(int coefficient, Variable variable) {
			checkArgument(variable.enclosingInstance() == ILPSolver.this);
			checkArgument(!coefficients.containsKey(variable));
			if (coefficient == 0)
				return this;
			return new LinearExpr(ImmutableMap.<Variable, Integer>builder()
					.putAll(coefficients).put(variable, coefficient).build());
		}
		public LinearExpr minus(int coefficient, Variable variable) {
			return plus(IntMath.checkedMultiply(coefficient, -1), variable);
		}
	}

	public final class Constraint {
		private final LinearExpr expr;
		private final int signum, value;
		private Constraint(LinearExpr expr, int signum, int value) {
			this.expr = expr;
			this.signum = signum;
			this.value = value;
		}
	}

	public final class ObjectiveFunction {
		private final LinearExpr expr;
		private final int signum;
		private ObjectiveFunction(LinearExpr expr, int signum) {
			this.expr = expr;
			this.signum = signum;
		}
	}

	public static void main(String[] args) {
		ILPSolver solver = new ILPSolver();
		Variable x = solver.newVariable("x"), y = solver.newVariable("y");
		LinearExpr lhs = x.asLinearExpr(10).plus(6, y);
		Constraint constraint = solver.constrainAtMost(lhs, 32);
		ObjectiveFunction objective = solver.maximize(lhs);
		solver.solve();
		System.out.println(x.value()+" "+y.value());
	}
}
