package edu.mit.streamjit.test.apps;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Pipeline;
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

/**
 * Ported from streamit/streams/apps/benchmarks/asplos06/serpent_full/streamit/Serpent_full.str
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
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
			add(new Splitjoin<>(new WeightedRoundrobinSplitter<Integer>(NBITS, 0), new RoundrobinJoiner<Integer>(),
					new Identity<Integer>(),
					new KeySchedule(round)));
			add(new Xor(2));
			add(new Sbox(round % 8));
			if (round < MAXROUNDS - 1)
				add(new rawL());
			else {
				add(new Splitjoin<>(new WeightedRoundrobinSplitter<Integer>(NBITS, 0), new RoundrobinJoiner<Integer>(),
					new Identity<Integer>(),
					new KeySchedule(MAXROUNDS)));
				add(new Xor(2));
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
			push(peek(16)^peek(52)^peek(56)^peek(70)^peek(83)^peek(94)^peek(105));
			push(peek(72)^peek(114)^peek(125));
			push(peek(2)^peek(9)^peek(15)^peek(30)^peek(76)^peek(84)^peek(126) );
			push(peek(36)^peek(90)^peek(103));
			push(peek(20)^peek(56)^peek(60)^peek(74)^peek(87)^peek(98)^peek(109) );
			push(peek(1)^peek(76)^peek(118) );
			push(peek(2)^peek(6)^peek(13)^peek(19)^peek(34)^peek(80)^peek(88) );
			push(peek(40)^peek(94)^peek(107));
			push(peek(24)^peek(60)^peek(64)^peek(78)^peek(91)^peek(102)^peek(113) );
			push(peek(5)^peek(80)^peek(122) );
			push(peek(6)^peek(10)^peek(17)^peek(23)^peek(38)^peek(84)^peek(92) );
			push(peek(44)^peek(98)^peek(111));
			push(peek(28)^peek(64)^peek(68)^peek(82)^peek(95)^peek(106)^peek(117) );
			push(peek(9)^peek(84)^peek(126) );
			push(peek(10)^peek(14)^peek(21)^peek(27)^peek(42)^peek(88)^peek(96) );
			push(peek(48)^peek(102)^peek(115));
			push(peek(32)^peek(68)^peek(72)^peek(86)^peek(99)^peek(110)^peek(121) );
			push(peek(2)^peek(13)^peek(88) );
			push(peek(14)^peek(18)^peek(25)^peek(31)^peek(46)^peek(92)^peek(100) );
			push(peek(52)^peek(106)^peek(119));
			push(peek(36)^peek(72)^peek(76)^peek(90)^peek(103)^peek(114)^peek(125) );
			push(peek(6)^peek(17)^peek(92) );
			push(peek(18)^peek(22)^peek(29)^peek(35)^peek(50)^peek(96)^peek(104) );
			push(peek(56)^peek(110)^peek(123));
			push(peek(1)^peek(40)^peek(76)^peek(80)^peek(94)^peek(107)^peek(118) );
			push(peek(10)^peek(21)^peek(96) );
			push(peek(22)^peek(26)^peek(33)^peek(39)^peek(54)^peek(100)^peek(108) );
			push(peek(60)^peek(114)^peek(127));
			push(peek(5)^peek(44)^peek(80)^peek(84)^peek(98)^peek(111)^peek(122) );
			push(peek(14)^peek(25)^peek(100) );
			push(peek(26)^peek(30)^peek(37)^peek(43)^peek(58)^peek(104)^peek(112) );
			push(peek(3)^peek(118));
			push(peek(9)^peek(48)^peek(84)^peek(88)^peek(102)^peek(115)^peek(126) );
			push(peek(18)^peek(29)^peek(104) );
			push(peek(30)^peek(34)^peek(41)^peek(47)^peek(62)^peek(108)^peek(116) );
			push(peek(7)^peek(122));
			push(peek(2)^peek(13)^peek(52)^peek(88)^peek(92)^peek(106)^peek(119) );
			push(peek(22)^peek(33)^peek(108) );
			push(peek(34)^peek(38)^peek(45)^peek(51)^peek(66)^peek(112)^peek(120) );
			push(peek(11)^peek(126));
			push(peek(6)^peek(17)^peek(56)^peek(92)^peek(96)^peek(110)^peek(123));
			push(peek(26)^peek(37)^peek(112));
			push(peek(38)^peek(42)^peek(49)^peek(55)^peek(70)^peek(116)^peek(124));
			push(peek(2)^peek(15)^peek(76));
			push(peek(10)^peek(21)^peek(60)^peek(96)^peek(100)^peek(114)^peek(127));
			push(peek(30)^peek(41)^peek(116));
			push(peek(0)^peek(42)^peek(46)^peek(53)^peek(59)^peek(74)^peek(120));
			push(peek(6)^peek(19)^peek(80));
			push(peek(3)^peek(14)^peek(25)^peek(100)^peek(104)^peek(118));
			push(peek(34)^peek(45)^peek(120));
			push(peek(4)^peek(46)^peek(50)^peek(57)^peek(63)^peek(78)^peek(124));
			push(peek(10)^peek(23)^peek(84));
			push(peek(7)^peek(18)^peek(29)^peek(104)^peek(108)^peek(122));
			push(peek(38)^peek(49)^peek(124));
			push(peek(0)^peek(8)^peek(50)^peek(54)^peek(61)^peek(67)^peek(82));
			push(peek(14)^peek(27)^peek(88));
			push(peek(11)^peek(22)^peek(33)^peek(108)^peek(112)^peek(126));
			push(peek(0)^peek(42)^peek(53));
			push(peek(4)^peek(12)^peek(54)^peek(58)^peek(65)^peek(71)^peek(86));
			push(peek(18)^peek(31)^peek(92));
			push(peek(2)^peek(15)^peek(26)^peek(37)^peek(76)^peek(112)^peek(116));
			push(peek(4)^peek(46)^peek(57));
			push(peek(8)^peek(16)^peek(58)^peek(62)^peek(69)^peek(75)^peek(90));
			push(peek(22)^peek(35)^peek(96));
			push(peek(6)^peek(19)^peek(30)^peek(41)^peek(80)^peek(116)^peek(120));
			push(peek(8)^peek(50)^peek(61));
			push(peek(12)^peek(20)^peek(62)^peek(66)^peek(73)^peek(79)^peek(94));
			push(peek(26)^peek(39)^peek(100));
			push(peek(10)^peek(23)^peek(34)^peek(45)^peek(84)^peek(120)^peek(124));
			push(peek(12)^peek(54)^peek(65));
			push(peek(16)^peek(24)^peek(66)^peek(70)^peek(77)^peek(83)^peek(98));
			push(peek(30)^peek(43)^peek(104));
			push(peek(0)^peek(14)^peek(27)^peek(38)^peek(49)^peek(88)^peek(124));
			push(peek(16)^peek(58)^peek(69));
			push(peek(20)^peek(28)^peek(70)^peek(74)^peek(81)^peek(87)^peek(102));
			push(peek(34)^peek(47)^peek(108));
			push(peek(0)^peek(4)^peek(18)^peek(31)^peek(42)^peek(53)^peek(92));
			push(peek(20)^peek(62)^peek(73));
			push(peek(24)^peek(32)^peek(74)^peek(78)^peek(85)^peek(91)^peek(106));
			push(peek(38)^peek(51)^peek(112));
			push(peek(4)^peek(8)^peek(22)^peek(35)^peek(46)^peek(57)^peek(96));
			push(peek(24)^peek(66)^peek(77));
			push(peek(28)^peek(36)^peek(78)^peek(82)^peek(89)^peek(95)^peek(110));
			push(peek(42)^peek(55)^peek(116));
			push(peek(8)^peek(12)^peek(26)^peek(39)^peek(50)^peek(61)^peek(100));
			push(peek(28)^peek(70)^peek(81));
			push(peek(32)^peek(40)^peek(82)^peek(86)^peek(93)^peek(99)^peek(114));
			push(peek(46)^peek(59)^peek(120));
			push(peek(12)^peek(16)^peek(30)^peek(43)^peek(54)^peek(65)^peek(104));
			push(peek(32)^peek(74)^peek(85));
			push(peek(36)^peek(90)^peek(103)^peek(118));
			push(peek(50)^peek(63)^peek(124));
			push(peek(16)^peek(20)^peek(34)^peek(47)^peek(58)^peek(69)^peek(108));
			push(peek(36)^peek(78)^peek(89));
			push(peek(40)^peek(94)^peek(107)^peek(122));
			push(peek(0)^peek(54)^peek(67));
			push(peek(20)^peek(24)^peek(38)^peek(51)^peek(62)^peek(73)^peek(112));
			push(peek(40)^peek(82)^peek(93));
			push(peek(44)^peek(98)^peek(111)^peek(126));
			push(peek(4)^peek(58)^peek(71));
			push(peek(24)^peek(28)^peek(42)^peek(55)^peek(66)^peek(77)^peek(116));
			push(peek(44)^peek(86)^peek(97));
			push(peek(2)^peek(48)^peek(102)^peek(115));
			push(peek(8)^peek(62)^peek(75));
			push(peek(28)^peek(32)^peek(46)^peek(59)^peek(70)^peek(81)^peek(120));
			push(peek(48)^peek(90)^peek(101));
			push(peek(6)^peek(52)^peek(106)^peek(119));
			push(peek(12)^peek(66)^peek(79));
			push(peek(32)^peek(36)^peek(50)^peek(63)^peek(74)^peek(85)^peek(124));
			push(peek(52)^peek(94)^peek(105));
			push(peek(10)^peek(56)^peek(110)^peek(123));
			push(peek(16)^peek(70)^peek(83));
			push(peek(0)^peek(36)^peek(40)^peek(54)^peek(67)^peek(78)^peek(89));
			push(peek(56)^peek(98)^peek(109));
			push(peek(14)^peek(60)^peek(114)^peek(127));
			push(peek(20)^peek(74)^peek(87));
			push(peek(4)^peek(40)^peek(44)^peek(58)^peek(71)^peek(82)^peek(93));
			push(peek(60)^peek(102)^peek(113));
			push(peek(3)^peek(18)^peek(72)^peek(114)^peek(118)^peek(125));
			push(peek(24)^peek(78)^peek(91));
			push(peek(8)^peek(44)^peek(48)^peek(62)^peek(75)^peek(86)^peek(97));
			push(peek(64)^peek(106)^peek(117));
			push(peek(1)^peek(7)^peek(22)^peek(76)^peek(118)^peek(122));
			push(peek(28)^peek(82)^peek(95));
			push(peek(12)^peek(48)^peek(52)^peek(66)^peek(79)^peek(90)^peek(101));
			push(peek(68)^peek(110)^peek(121));
			push(peek(5)^peek(11)^peek(26)^peek(80)^peek(122)^peek(126));
			push(peek(32)^peek(86)^peek(99));
			for (int i = 0; i < 128; i++) {
				pop();
			}
		}
	}
}
