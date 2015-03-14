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
package edu.mit.streamjit.test.apps;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Rate;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.WeightedRoundrobinSplitter;
import edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmark.Dataset;
import edu.mit.streamjit.test.Benchmarker;
import edu.mit.streamjit.test.Datasets;
import edu.mit.streamjit.test.SuppliedBenchmark;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

/**
 * Ported from streamit/streams/apps/benchmarks/asplos06/serpent_full/streamit/Serpent_full.str
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 2/11/2014
 */
public final class SerpentFull {
	public static void main(String[] args) throws InterruptedException {
		StreamCompiler sc = new DebugStreamCompiler();
		Benchmarker.runBenchmark(new SerpentBenchmark(), sc).get(0).print(System.out);
	}

	@ServiceProvider(Benchmark.class)
	public static final class SerpentBenchmark extends SuppliedBenchmark {
		private static final int COPIES = 1;
		public SerpentBenchmark() {
			super("Serpent", SerpentEncoder.class, new Dataset("serpent.in", (Input)Datasets.nCopies(COPIES, (Input)Input.fromBinaryFile(Paths.get("data/serpent.in"), Integer.class, ByteOrder.LITTLE_ENDIAN))
					//, (Supplier)Suppliers.ofInstance((Input)Input.fromBinaryFile(Paths.get("/home/jbosboom/streamit/streams/apps/benchmarks/asplos06/serpent_full/streamit/serpent_full.out"), Integer.class, ByteOrder.LITTLE_ENDIAN))
			));
		}
	}

	private static final int testvector = 2;
	private static final int BITS_PER_WORD  = 32;
    // length of plain text, and cipher text
    private static final int NBITS          = 128;
    // used for key schedule (golden ration)
    private static final int PHI            = 0x9e3779b9;
	// algorithm has 32 total rounds
    private static final int MAXROUNDS      = 8;
	private static final int[][] USERKEYS = {// 0000000000000000000000000000000000000000000000000000000000000000
                          {0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000},
                          // 0000000000000000000000000000000000000000000000000000000000000000 (repeated purposefully for testing)
                          {0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000},
                          // 92efa3ca9477794d31f4df7bce23e60a6038d2d2710373f04fd30aaecea8aa43
                          {0x92efa3ca, 0x9477794d, 0x31f4df7b, 0xce23e60a, 0x6038d2d2, 0x710373f0, 0x4fd30aae, 0xcea8aa43},
                          // d3fc99e32d09420f00a041f7e32914747731be4d4e5b5da518c2abe0a1239fa8
                          {0xd3fc99e3, 0x2d09420f, 0x00a041f7, 0xe3291474, 0x7731be4d, 0x4e5b5da5, 0x18c2abe0, 0xa1239fa8},
                          // bd14742460c6addfc71eef1328e2ddb6ba5b8798bb66c3c4d380acb055cac569
                          {0xbd147424, 0x60c6addf, 0xc71eef13, 0x28e2ddb6, 0xba5b8798, 0xbb66c3c4, 0xd380acb0, 0x55cac569}};
	private static final int USERKEY_LENGTH = 8 * BITS_PER_WORD;
    // initial permutation
    private static final int[] IP = { 0, 32, 64, 96,   1, 33, 65, 97,   2, 34, 66, 98,   3, 35, 67, 99,
                    4, 36, 68, 100,  5, 37, 69, 101,  6, 38, 70, 102,  7, 39, 71, 103,
                    8, 40, 72, 104,  9, 41, 73, 105, 10, 42, 74, 106, 11, 43, 75, 107,
                   12, 44, 76, 108, 13, 45, 77, 109, 14, 46, 78, 110, 15, 47, 79, 111,
                   16, 48, 80, 112, 17, 49, 81, 113, 18, 50, 82, 114, 19, 51, 83, 115,
                   20, 52, 84, 116, 21, 53, 85, 117, 22, 54, 86, 118, 23, 55, 87, 119,
                   24, 56, 88, 120, 25, 57, 89, 121, 26, 58, 90, 122, 27, 59, 91, 123,
                   28, 60, 92, 124, 29, 61, 93, 125, 30, 62, 94, 126, 31, 63, 95, 127};
    // final permutation
    private static final int[] FP = { 0,  4,  8, 12, 16, 20, 24, 28, 32,  36,  40,  44,  48,  52,  56,  60,
                   64, 68, 72, 76, 80, 84, 88, 92, 96, 100, 104, 108, 112, 116, 120, 124,
                    1,  5,  9, 13, 17, 21, 25, 29, 33,  37,  41,  45,  49,  53,  57,  61,
                   65, 69, 73, 77, 81, 85, 89, 93, 97, 101, 105, 109, 113, 117, 121, 125,
                    2,  6, 10, 14, 18, 22, 26, 30, 34,  38,  42,  46,  50,  54,  58,  62,
                   66, 70, 74, 78, 82, 86, 90, 94, 98, 102, 106, 110, 114, 118, 122, 126,
                    3,  7, 11, 15, 19, 23, 27, 31, 35,  39,  43,  47,  51,  55,  59,  63,
                   67, 71, 75, 79, 83, 87, 91, 95, 99, 103, 107, 111, 115, 119, 123, 127};
    // substitution boxes
    private static final int[][] SBOXES = {{ 3,  8, 15,  1, 10,  6,  5, 11, 14, 13,  4,  2,  7,  0,  9, 12 }, /* S0: */
                         {15, 12,  2,  7,  9,  0,  5, 10,  1, 11, 14,  8,  6, 13,  3,  4 }, /* S1: */
                         { 8,  6,  7,  9,  3, 12, 10, 15, 13,  1, 14,  4,  0, 11,  5,  2 }, /* S2: */
                         { 0, 15, 11,  8, 12,  9,  6,  3, 13,  1,  2,  4, 10,  7,  5, 14 }, /* S3: */
                         { 1, 15,  8,  3, 12,  0, 11,  6,  2,  5,  4, 10,  9, 14,  7, 13 }, /* S4: */
                         {15,  5,  2, 11,  4, 10,  9, 12,  0,  3, 14,  8, 13,  6,  7,  1 }, /* S5: */
                         { 7,  2, 12,  5,  8,  4,  6, 11, 14,  9,  1, 15, 13,  3, 10,  0 }, /* S6: */
                         { 1, 13, 15,  0, 14,  8,  2, 11,  7,  4, 12, 10,  9,  3,  5,  6 }};/* S7: */

	public static final class SerpentEncoder extends Pipeline<Integer, Integer> {
		public SerpentEncoder() {
			super();
			add(new Permute(NBITS, IP));
			for (int i = 0; i < MAXROUNDS; ++i)
				add(new R(i));
			add(new Permute(NBITS, FP));
		}
	}

	private static final class Permute extends Filter<Integer, Integer> {
		private final int[] permutation;
		private Permute(int n, int[] permutation) {
			super(n, n, n);
			this.permutation = permutation;
			assert permutation.length == n;
		}
		@Override
		public void work() {
			for (int i = 0; i < permutation.length; ++i)
				push(peek(permutation[i]));
			for (int i = 0; i < permutation.length; ++i)
				pop();
		}
	}

	private static final class R extends Pipeline<Integer, Integer> {
		private R(int round) {
			super();
			add(new Splitjoin<>(new WeightedRoundrobinSplitter<Integer>(NBITS, 0), new XorJoiner(2),
					new Identity<Integer>(),
					new KeySchedule(round)));
//			add(new Xor(2));
			add(new Sbox(round % 8));
			if (round < MAXROUNDS - 1)
				add(new rawL());
			else {
				add(new Splitjoin<>(new WeightedRoundrobinSplitter<Integer>(NBITS, 0), new XorJoiner(2),
					new Identity<Integer>(),
					new KeySchedule(MAXROUNDS)));
//				add(new Xor(2));
			}
		}
	}

	private static final class KeySchedule extends Filter<Integer, Integer> {
		private static final int[][] keys = new int[MAXROUNDS+1][NBITS];

		static {
			// precalculate key schedule
			int[]   userkey = {0, 0, 0, 0, 0, 0, 0, 0}; // initialize to 0
            int[] w = new int[140];

            int words = USERKEY_LENGTH / BITS_PER_WORD;
            for (int i = words - 1; i >= 0; i--)
                userkey[words - 1 - i] = USERKEYS[testvector][i];

            // add 1 to MSB of user key
            if (USERKEY_LENGTH < 256) {
                int msb = userkey[USERKEY_LENGTH / BITS_PER_WORD];
                userkey[USERKEY_LENGTH / BITS_PER_WORD] = msb | 1 << (USERKEY_LENGTH % BITS_PER_WORD);
            }

            // make prekeys w_-8 ... w_-1
            for (int i = 0; i < 8; i++)
                w[i] = userkey[i];

            // calculate intermediate keys w_0 ... w_131
            for (int i = 8; i < 140; i++) {
                w[i] = w[i - 8] ^ w[i - 5] ^ w[i - 3] ^ w[i - 1] ^ PHI ^ (i - 8);
                w[i] = Integer.rotateLeft(w[i], 11);
            }

            // calculate keys for round 0 - 32
            for (int i = 0; i <= MAXROUNDS; i++) {
                int[] sbox = new int[BITS_PER_WORD];
                for (int b = 0; b < BITS_PER_WORD; b++) {
                    // int to bits in slices
                    int r  = (4 * i) + 8;
                    int b0 = (w[r + 0] & (1 << b)) >> b;
                    int b1 = (w[r + 1] & (1 << b)) >> b;
                    int b2 = (w[r + 2] & (1 << b)) >> b;
                    int b3 = (w[r + 3] & (1 << b)) >> b;

                    int val = 0;
                    if (b0 != 0) val = 1;
                    if (b1 != 0) val = val | (1 << 1);
                    if (b2 != 0) val = val | (1 << 2);
                    if (b3 != 0) val = val | (1 << 3);

                    // round  0: use sbox 3
                    // round  1: use sbox 2
                    // round  2: use sbox 1
                    // round  3: use sbox 0
                    // round  4: use sbox 7
                    // ...
                    // round 31: use sbox 4
                    // round 32: use sbox 3
                    sbox[b] = SBOXES[(32 + 3 - i) % 8][val];
                }

                // reverse bit slice and store bits
                int[] key = new int[NBITS];
                for (int k = 0; k < NBITS / BITS_PER_WORD; k++) {
                    for (int b = 0; b < BITS_PER_WORD; b++) {
                        int x = (sbox[b] & (1 << k)) >> k;
                        if (x != 0)
                            key[(k * BITS_PER_WORD) + b] = 1;
                        else
                            key[(k * BITS_PER_WORD) + b] = 0;
                    }
                }

                // perform initial permutation (IP)
                for (int b = 0; b < NBITS; b++) {
                    keys[i][b] = key[IP[b]];
                }
            }
		}
		private final int[] roundKeys;
		private KeySchedule(int round) {
			super(0, NBITS);
			this.roundKeys = keys[round];
		}
		@Override
		public void work() {
			for (int i = 0; i < roundKeys.length; ++i)
				push(roundKeys[i]);
		}
	}

	private static final class Xor extends Filter<Integer, Integer> {
		private final int n;
		private Xor(int n) {
			super(n, 1);
			this.n = n;
		}
		@Override
		public void work() {
			int x = pop();
			for (int i = 1; i < n; i++)
				x ^= pop();
			push(x);
		}
	}

	private static final class XorJoiner extends Joiner<Integer, Integer> {
		private final int n;
		private XorJoiner(int n) {
			this.n = n;
		}
		@Override
		public int supportedInputs() {
			 return n;
		}
		@Override
		public void work() {
			int x = 0;
			for (int i = 0; i < n; ++i)
				x ^= pop(i);
			push(x);
		}
		@Override
		public List<Rate> getPeekRates() {
			return Collections.nCopies(n, Rate.create(0));
		}
		@Override
		public List<Rate> getPopRates() {
			return Collections.nCopies(n, Rate.create(1));
		}
		@Override
		public List<Rate> getPushRates() {
			return Collections.singletonList(Rate.create(1));
		}
	}

	private static final class Sbox extends Filter<Integer, Integer> {
		private final int[] sbox;
		private Sbox(int round) {
			super(4, 4);
			this.sbox = SBOXES[round];
		}
		@Override
		public void work() {
			int val = pop();
			val = (pop() << 1) | val;
			val = (pop() << 2) | val;
			val = (pop() << 3) | val;

			int out = sbox[val];
			push((int)((out & 0x1) >> 0));
			push((int)((out & 0x2) >> 1));
			push((int)((out & 0x4) >> 2));
			push((int)((out & 0x8) >> 3));
		}
	}

	private static final class rawL extends Filter<Integer, Integer> {
		private rawL() {
			super(128, 128, 128);
		}
		@Override
		public void work() {
			//TODO: teach the compiler to lift peeks to arrays/local variables
			//for the common case of all-peek-then-all-pop filters.  Hopefully
			//we'd then understand to make one array per thread instead of one
			//array per work() call, but maybe HotSpot will scalar-replace.
			int[] array = new int[128];
			for (int i = 0; i < array.length; i++) {
				array[i] = pop();
			}
			push(array[16]^array[52]^array[56]^array[70]^array[83]^array[94]^array[105]);
			push(array[72]^array[114]^array[125]);
			push(array[2]^array[9]^array[15]^array[30]^array[76]^array[84]^array[126] );
			push(array[36]^array[90]^array[103]);
			push(array[20]^array[56]^array[60]^array[74]^array[87]^array[98]^array[109] );
			push(array[1]^array[76]^array[118] );
			push(array[2]^array[6]^array[13]^array[19]^array[34]^array[80]^array[88] );
			push(array[40]^array[94]^array[107]);
			push(array[24]^array[60]^array[64]^array[78]^array[91]^array[102]^array[113] );
			push(array[5]^array[80]^array[122] );
			push(array[6]^array[10]^array[17]^array[23]^array[38]^array[84]^array[92] );
			push(array[44]^array[98]^array[111]);
			push(array[28]^array[64]^array[68]^array[82]^array[95]^array[106]^array[117] );
			push(array[9]^array[84]^array[126] );
			push(array[10]^array[14]^array[21]^array[27]^array[42]^array[88]^array[96] );
			push(array[48]^array[102]^array[115]);
			push(array[32]^array[68]^array[72]^array[86]^array[99]^array[110]^array[121] );
			push(array[2]^array[13]^array[88] );
			push(array[14]^array[18]^array[25]^array[31]^array[46]^array[92]^array[100] );
			push(array[52]^array[106]^array[119]);
			push(array[36]^array[72]^array[76]^array[90]^array[103]^array[114]^array[125] );
			push(array[6]^array[17]^array[92] );
			push(array[18]^array[22]^array[29]^array[35]^array[50]^array[96]^array[104] );
			push(array[56]^array[110]^array[123]);
			push(array[1]^array[40]^array[76]^array[80]^array[94]^array[107]^array[118] );
			push(array[10]^array[21]^array[96] );
			push(array[22]^array[26]^array[33]^array[39]^array[54]^array[100]^array[108] );
			push(array[60]^array[114]^array[127]);
			push(array[5]^array[44]^array[80]^array[84]^array[98]^array[111]^array[122] );
			push(array[14]^array[25]^array[100] );
			push(array[26]^array[30]^array[37]^array[43]^array[58]^array[104]^array[112] );
			push(array[3]^array[118]);
			push(array[9]^array[48]^array[84]^array[88]^array[102]^array[115]^array[126] );
			push(array[18]^array[29]^array[104] );
			push(array[30]^array[34]^array[41]^array[47]^array[62]^array[108]^array[116] );
			push(array[7]^array[122]);
			push(array[2]^array[13]^array[52]^array[88]^array[92]^array[106]^array[119] );
			push(array[22]^array[33]^array[108] );
			push(array[34]^array[38]^array[45]^array[51]^array[66]^array[112]^array[120] );
			push(array[11]^array[126]);
			push(array[6]^array[17]^array[56]^array[92]^array[96]^array[110]^array[123]);
			push(array[26]^array[37]^array[112]);
			push(array[38]^array[42]^array[49]^array[55]^array[70]^array[116]^array[124]);
			push(array[2]^array[15]^array[76]);
			push(array[10]^array[21]^array[60]^array[96]^array[100]^array[114]^array[127]);
			push(array[30]^array[41]^array[116]);
			push(array[0]^array[42]^array[46]^array[53]^array[59]^array[74]^array[120]);
			push(array[6]^array[19]^array[80]);
			push(array[3]^array[14]^array[25]^array[100]^array[104]^array[118]);
			push(array[34]^array[45]^array[120]);
			push(array[4]^array[46]^array[50]^array[57]^array[63]^array[78]^array[124]);
			push(array[10]^array[23]^array[84]);
			push(array[7]^array[18]^array[29]^array[104]^array[108]^array[122]);
			push(array[38]^array[49]^array[124]);
			push(array[0]^array[8]^array[50]^array[54]^array[61]^array[67]^array[82]);
			push(array[14]^array[27]^array[88]);
			push(array[11]^array[22]^array[33]^array[108]^array[112]^array[126]);
			push(array[0]^array[42]^array[53]);
			push(array[4]^array[12]^array[54]^array[58]^array[65]^array[71]^array[86]);
			push(array[18]^array[31]^array[92]);
			push(array[2]^array[15]^array[26]^array[37]^array[76]^array[112]^array[116]);
			push(array[4]^array[46]^array[57]);
			push(array[8]^array[16]^array[58]^array[62]^array[69]^array[75]^array[90]);
			push(array[22]^array[35]^array[96]);
			push(array[6]^array[19]^array[30]^array[41]^array[80]^array[116]^array[120]);
			push(array[8]^array[50]^array[61]);
			push(array[12]^array[20]^array[62]^array[66]^array[73]^array[79]^array[94]);
			push(array[26]^array[39]^array[100]);
			push(array[10]^array[23]^array[34]^array[45]^array[84]^array[120]^array[124]);
			push(array[12]^array[54]^array[65]);
			push(array[16]^array[24]^array[66]^array[70]^array[77]^array[83]^array[98]);
			push(array[30]^array[43]^array[104]);
			push(array[0]^array[14]^array[27]^array[38]^array[49]^array[88]^array[124]);
			push(array[16]^array[58]^array[69]);
			push(array[20]^array[28]^array[70]^array[74]^array[81]^array[87]^array[102]);
			push(array[34]^array[47]^array[108]);
			push(array[0]^array[4]^array[18]^array[31]^array[42]^array[53]^array[92]);
			push(array[20]^array[62]^array[73]);
			push(array[24]^array[32]^array[74]^array[78]^array[85]^array[91]^array[106]);
			push(array[38]^array[51]^array[112]);
			push(array[4]^array[8]^array[22]^array[35]^array[46]^array[57]^array[96]);
			push(array[24]^array[66]^array[77]);
			push(array[28]^array[36]^array[78]^array[82]^array[89]^array[95]^array[110]);
			push(array[42]^array[55]^array[116]);
			push(array[8]^array[12]^array[26]^array[39]^array[50]^array[61]^array[100]);
			push(array[28]^array[70]^array[81]);
			push(array[32]^array[40]^array[82]^array[86]^array[93]^array[99]^array[114]);
			push(array[46]^array[59]^array[120]);
			push(array[12]^array[16]^array[30]^array[43]^array[54]^array[65]^array[104]);
			push(array[32]^array[74]^array[85]);
			push(array[36]^array[90]^array[103]^array[118]);
			push(array[50]^array[63]^array[124]);
			push(array[16]^array[20]^array[34]^array[47]^array[58]^array[69]^array[108]);
			push(array[36]^array[78]^array[89]);
			push(array[40]^array[94]^array[107]^array[122]);
			push(array[0]^array[54]^array[67]);
			push(array[20]^array[24]^array[38]^array[51]^array[62]^array[73]^array[112]);
			push(array[40]^array[82]^array[93]);
			push(array[44]^array[98]^array[111]^array[126]);
			push(array[4]^array[58]^array[71]);
			push(array[24]^array[28]^array[42]^array[55]^array[66]^array[77]^array[116]);
			push(array[44]^array[86]^array[97]);
			push(array[2]^array[48]^array[102]^array[115]);
			push(array[8]^array[62]^array[75]);
			push(array[28]^array[32]^array[46]^array[59]^array[70]^array[81]^array[120]);
			push(array[48]^array[90]^array[101]);
			push(array[6]^array[52]^array[106]^array[119]);
			push(array[12]^array[66]^array[79]);
			push(array[32]^array[36]^array[50]^array[63]^array[74]^array[85]^array[124]);
			push(array[52]^array[94]^array[105]);
			push(array[10]^array[56]^array[110]^array[123]);
			push(array[16]^array[70]^array[83]);
			push(array[0]^array[36]^array[40]^array[54]^array[67]^array[78]^array[89]);
			push(array[56]^array[98]^array[109]);
			push(array[14]^array[60]^array[114]^array[127]);
			push(array[20]^array[74]^array[87]);
			push(array[4]^array[40]^array[44]^array[58]^array[71]^array[82]^array[93]);
			push(array[60]^array[102]^array[113]);
			push(array[3]^array[18]^array[72]^array[114]^array[118]^array[125]);
			push(array[24]^array[78]^array[91]);
			push(array[8]^array[44]^array[48]^array[62]^array[75]^array[86]^array[97]);
			push(array[64]^array[106]^array[117]);
			push(array[1]^array[7]^array[22]^array[76]^array[118]^array[122]);
			push(array[28]^array[82]^array[95]);
			push(array[12]^array[48]^array[52]^array[66]^array[79]^array[90]^array[101]);
			push(array[68]^array[110]^array[121]);
			push(array[5]^array[11]^array[26]^array[80]^array[122]^array[126]);
			push(array[32]^array[86]^array[99]);
		}
	}
}
