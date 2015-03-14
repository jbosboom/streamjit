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
package edu.mit.streamjit.util;

import static com.google.common.math.LongMath.gcd;
import static com.google.common.math.LongMath.checkedAdd;
import static com.google.common.math.LongMath.checkedMultiply;

/**
 * Represents a Fraction.
 *
 * Imported from Jeffrey's Project Euler project on 4/30/2013.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 10/1/2011
 */
public final class Fraction extends Number implements Comparable<Fraction> {
	private static final long serialVersionUID = 1L;
	public static final Fraction ZERO = new Fraction(0);
	public static final Fraction ONE = new Fraction(1);
	private final long numerator, denominator;
	public Fraction(long numerator) {
		this(numerator, 1);
	}
	public Fraction(long numerator, long denominator) {
		if (denominator == 0)
			throw new ArithmeticException(numerator+"/"+denominator);

		if (numerator == 0)
			denominator = 1;
		else {
			long gcd = gcd(Math.abs(numerator), Math.abs(denominator));
			numerator /= gcd;
			denominator /= gcd;

			if (denominator < 0) {
				numerator = checkedNegate(numerator);
				denominator = checkedNegate(denominator);
			}
		}

		this.numerator = numerator;
		this.denominator = denominator;
	}

	public long num() {
		return numerator;
	}

	public long denom() {
		return denominator;
	}

	public Fraction add(Fraction other) {
		//TODO: use LCM here to be more overflow-resistant
		return new Fraction(checkedAdd(checkedMultiply(num(), other.denom()), checkedMultiply(other.num(), denom())), checkedMultiply(denom(), other.denom()));
	}

	public Fraction sub(Fraction other) {
		return this.add(other.neg());
	}

	public Fraction mul(Fraction other) {
		return new Fraction(checkedMultiply(num(), other.num()), checkedMultiply(denom(), other.denom()));
	}

	public Fraction div(Fraction other) {
		return this.mul(other.recip());
	}

	public Fraction neg() {
		return new Fraction(checkedNegate(numerator), denominator);
	}

	public Fraction recip() {
		return new Fraction(denominator, numerator);
	}

	/**
	 * If this Fraction represents a mathematical integer, returns that integer
	 * (the numerator).  Throws ArithmeticException otherwise.
	 *
	 * This might be considered a checked version of {@code #longValue()}.
	 * @return this fraction's value if it's an integer
	 */
	public long asInteger() {
		if (denom() != 1)
			throw new ArithmeticException("Not an integer: "+toString());
		return num();
	}

	@Override
	public int intValue() {
		return (int)doubleValue();
	}

	@Override
	public long longValue() {
		return (long)doubleValue();
	}

	@Override
	public float floatValue() {
		return (float)doubleValue();
	}

	@Override
	public double doubleValue() {
		return ((double)num())/denom();
	}

	@Override
	public int compareTo(Fraction other) {
		return Long.compare(num()*other.denom(), other.num()*denom());
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final Fraction other = (Fraction)obj;
		if (this.numerator != other.numerator)
			return false;
		if (this.denominator != other.denominator)
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		int hash = 7;
		hash = 13 * hash + (int)(this.numerator ^ (this.numerator >>> 32));
		hash = 13 * hash + (int)(this.denominator ^ (this.denominator >>> 32));
		return hash;
	}

	@Override
	public String toString() {
		return num()+"/"+denom();
	}

	private static long checkedNegate(long x) {
		if (x == Long.MIN_VALUE)
			throw new ArithmeticException();
		return -x;
	}
}
