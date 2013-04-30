package edu.mit.streamjit.util;

import com.google.common.math.LongMath;

/**
 * Represents a Fraction.
 *
 * Imported from Jeffrey's Project Euler project on 4/30/2013.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 10/1/2011
 */
public final class Fraction implements Comparable<Fraction> {
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
			long gcd = LongMath.gcd(Math.abs(numerator), Math.abs(denominator));
			numerator /= gcd;
			denominator /= gcd;

			if (denominator < 0) {
				numerator = -numerator;
				denominator = -denominator;
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
		return new Fraction(num()*other.denom() + other.num()*denom(), denom()*other.denom());
	}

	public Fraction sub(Fraction other) {
		return this.add(other.neg());
	}

	public Fraction mul(Fraction other) {
		return new Fraction(num()*other.num(), denom()*other.denom());
	}

	public Fraction div(Fraction other) {
		return this.mul(other.recip());
	}

	public Fraction neg() {
		return new Fraction(-numerator, denominator);
	}

	public Fraction recip() {
		return new Fraction(denominator, numerator);
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
}
