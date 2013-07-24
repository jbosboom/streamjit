package edu.mit.streamjit.apps.des2;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.DuplicateSplitter;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;

/**
 * Rewritten StreamIt's asplos06 benchmarks. Refer STREAMIT_HOME/apps/benchmarks/asplos06/des/streamit/DES2.str for original
 * implementations. Each StreamIt's language constructs (i.e., pipeline, filter and splitjoin) are rewritten as classes in StreamJit.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Mar 13, 2013
 */
public class DES2 {

	// used for printing descriptor in output
	public static boolean PRINTINFO = false;
	public static int PLAINTEXT = 0;
	public static int USERKEY = 1;
	public static int CIPHERTEXT = 2;

	// algorithm has 16 total rounds
	public static int MAXROUNDS = 4;

	// sample user keys
	// int[34][2] USERKEYS
	public static int[][] USERKEYS = { { 0x00000000, 0x00000000 }, // 0x0000000000000000
			{ 0xFFFFFFFF, 0xFFFFFFFF }, // 0xFFFFFFFFFFFFFFFF
			{ 0x30000000, 0x00000000 }, // 0x3000000000000000
			{ 0x11111111, 0x11111111 }, // 0x1111111111111111
			{ 0x01234567, 0x89ABCDEF }, // 0x0123456789ABCDEF
			{ 0x11111111, 0x11111111 }, // 0x1111111111111111
			{ 0x00000000, 0x00000000 }, // 0x0000000000000000
			{ 0xFEDCBA98, 0x76543210 }, // 0xFEDCBA9876543210
			{ 0x7CA11045, 0x4A1A6E57 }, // 0x7CA110454A1A6E57
			{ 0x0131D961, 0x9DC1376E }, // 0x0131D9619DC1376E
			{ 0x07A1133E, 0x4A0B2686 }, // 0x07A1133E4A0B2686
			{ 0x3849674C, 0x2602319E }, // 0x3849674C2602319E
			{ 0x04B915BA, 0x43FEB5B6 }, // 0x04B915BA43FEB5B6
			{ 0x0113B970, 0xFD34F2CE }, // 0x0113B970FD34F2CE
			{ 0x0170F175, 0x468FB5E6 }, // 0x0170F175468FB5E6
			{ 0x43297FAD, 0x38E373FE }, // 0x43297FAD38E373FE
			{ 0x07A71370, 0x45DA2A16 }, // 0x07A7137045DA2A16
			{ 0x04689104, 0xC2FD3B2F }, // 0x04689104C2FD3B2F
			{ 0x37D06BB5, 0x16CB7546 }, // 0x37D06BB516CB7546
			{ 0x1F08260D, 0x1AC2465E }, // 0x1F08260D1AC2465E
			{ 0x58402364, 0x1ABA6176 }, // 0x584023641ABA6176
			{ 0x02581616, 0x4629B007 }, // 0x025816164629B007
			{ 0x49793EBC, 0x79B3258F }, // 0x49793EBC79B3258F
			{ 0x4FB05E15, 0x15AB73A7 }, // 0x4FB05E1515AB73A7
			{ 0x49E95D6D, 0x4CA229BF }, // 0x49E95D6D4CA229BF
			{ 0x018310DC, 0x409B26D6 }, // 0x018310DC409B26D6
			{ 0x1C587F1C, 0x13924FEF }, // 0x1C587F1C13924FEF
			{ 0x01010101, 0x01010101 }, // 0x0101010101010101
			{ 0x1F1F1F1F, 0x0E0E0E0E }, // 0x1F1F1F1F0E0E0E0E
			{ 0xE0FEE0FE, 0xF1FEF1FE }, // 0xE0FEE0FEF1FEF1FE
			{ 0x00000000, 0x00000000 }, // 0x0000000000000000
			{ 0xFFFFFFFF, 0xFFFFFFFF }, // 0xFFFFFFFFFFFFFFFF
			{ 0x01234567, 0x89ABCDEF }, // 0x0123456789ABCDEF
			{ 0xFEDCBA98, 0x76543210 } }; // 0xFEDCBA9876543210

	// PC1 permutation for key schedule
	// int[56] PC1
	public static int[] PC1 = { 57, 49, 41, 33, 25, 17, 9, 1, 58, 50, 42, 34, 26, 18, 10, 2, 59, 51, 43, 35, 27, 19, 11, 3, 60, 52,
			44, 36, 63, 55, 47, 39, 31, 23, 15, 7, 62, 54, 46, 38, 30, 22, 14, 6, 61, 53, 45, 37, 29, 21, 13, 5, 28, 20, 12, 4 };

	// PC2 permutation for key schedule
	// int[48] PC2
	public static int[] PC2 = { 14, 17, 11, 24, 1, 5, 3, 28, 15, 6, 21, 10, 23, 19, 12, 4, 26, 8, 16, 7, 27, 20, 13, 2, 41, 52, 31,
			37, 47, 55, 30, 40, 51, 45, 33, 48, 44, 49, 39, 56, 34, 53, 46, 42, 50, 36, 29, 32 };

	// key rotation table for key schedule
	// int[16] RT
	public static int[] RT = { 1, 1, 2, 2, 2, 2, 2, 2, 1, 2, 2, 2, 2, 2, 2, 1 };

	// initial permuation
	// int[64] IP
	public static int[] IP = { 58, 50, 42, 34, 26, 18, 10, 2, 60, 52, 44, 36, 28, 20, 12, 4, 62, 54, 46, 38, 30, 22, 14, 6, 64, 56,
			48, 40, 32, 24, 16, 8, 57, 49, 41, 33, 25, 17, 9, 1, 59, 51, 43, 35, 27, 19, 11, 3, 61, 53, 45, 37, 29, 21, 13, 5, 63, 55,
			47, 39, 31, 23, 15, 7 };

	// expansion permutation (bit selection)
	// int[48] E
	public static int[] E = { 32, 1, 2, 3, 4, 5, 4, 5, 6, 7, 8, 9, 8, 9, 10, 11, 12, 13, 12, 13, 14, 15, 16, 17, 16, 17, 18, 19, 20,
			21, 20, 21, 22, 23, 24, 25, 24, 25, 26, 27, 28, 29, 28, 29, 30, 31, 32, 1 };

	// P permutation of sbox output
	// int[32] P
	public static int[] P = { 16, 7, 20, 21, 29, 12, 28, 17, 1, 15, 23, 26, 5, 18, 31, 10, 2, 8, 24, 14, 32, 27, 3, 9, 19, 13, 30, 6,
			22, 11, 4, 25 };

	// inverse intial permuation
	// int[64] IPm1
	public static int[] IPm1 = { 40, 8, 48, 16, 56, 24, 64, 32, 39, 7, 47, 15, 55, 23, 63, 31, 38, 6, 46, 14, 54, 22, 62, 30, 37, 5,
			45, 13, 53, 21, 61, 29, 36, 4, 44, 12, 52, 20, 60, 28, 35, 3, 43, 11, 51, 19, 59, 27, 34, 2, 42, 10, 50, 18, 58, 26, 33,
			1, 41, 9, 49, 17, 57, 25 };

	// provides sbox permutations for DES encryption
	// int[4][16] S1
	public static int[][] S1 = { { 14, 4, 13, 1, 2, 15, 11, 8, 3, 10, 6, 12, 5, 9, 0, 7 },
			{ 0, 15, 7, 4, 14, 2, 13, 1, 10, 6, 12, 11, 9, 5, 3, 8 }, { 4, 1, 14, 8, 13, 6, 2, 11, 15, 12, 9, 7, 3, 10, 5, 0 },
			{ 15, 12, 8, 2, 4, 9, 1, 7, 5, 11, 3, 14, 10, 0, 6, 13 } };

	// int[4][16] S2
	public static int[][] S2 = { { 15, 1, 8, 14, 6, 11, 3, 4, 9, 7, 2, 13, 12, 0, 5, 10 },
			{ 3, 13, 4, 7, 15, 2, 8, 14, 12, 0, 1, 10, 6, 9, 11, 5 }, { 0, 14, 7, 11, 10, 4, 13, 1, 5, 8, 12, 6, 9, 3, 2, 15 },
			{ 13, 8, 10, 1, 3, 15, 4, 2, 11, 6, 7, 12, 0, 5, 14, 9 } };

	// int[4][16] S3
	public static int[][] S3 = { { 10, 0, 9, 14, 6, 3, 15, 5, 1, 13, 12, 7, 11, 4, 2, 8 },
			{ 13, 7, 0, 9, 3, 4, 6, 10, 2, 8, 5, 14, 12, 11, 15, 1 }, { 13, 6, 4, 9, 8, 15, 3, 0, 11, 1, 2, 12, 5, 10, 14, 7 },
			{ 1, 10, 13, 0, 6, 9, 8, 7, 4, 15, 14, 3, 11, 5, 2, 12 } };

	// int[4][16] S4
	public static int[][] S4 = { { 7, 13, 14, 3, 0, 6, 9, 10, 1, 2, 8, 5, 11, 12, 4, 15 },
			{ 13, 8, 11, 5, 6, 15, 0, 3, 4, 7, 2, 12, 1, 10, 14, 9 }, { 10, 6, 9, 0, 12, 11, 7, 13, 15, 1, 3, 14, 5, 2, 8, 4 },
			{ 3, 15, 0, 6, 10, 1, 13, 8, 9, 4, 5, 11, 12, 7, 2, 14 } };

	// int[4][16] S5
	public static int[][] S5 = { { 2, 12, 4, 1, 7, 10, 11, 6, 8, 5, 3, 15, 13, 0, 14, 9 },
			{ 14, 11, 2, 12, 4, 7, 13, 1, 5, 0, 15, 10, 3, 9, 8, 6 }, { 4, 2, 1, 11, 10, 13, 7, 8, 15, 9, 12, 5, 6, 3, 0, 14 },
			{ 11, 8, 12, 7, 1, 14, 2, 13, 6, 15, 0, 9, 10, 4, 5, 3 } };

	// int[4][16] S6
	public static int[][] S6 = { { 12, 1, 10, 15, 9, 2, 6, 8, 0, 13, 3, 4, 14, 7, 5, 11 },
			{ 10, 15, 4, 2, 7, 12, 9, 5, 6, 1, 13, 14, 0, 11, 3, 8 }, { 9, 14, 15, 5, 2, 8, 12, 3, 7, 0, 4, 10, 1, 13, 11, 6 },
			{ 4, 3, 2, 12, 9, 5, 15, 10, 11, 14, 1, 7, 6, 0, 8, 13 } };
	// int[4][16] S7
	public static int[][] S7 = { { 4, 11, 2, 14, 15, 0, 8, 13, 3, 12, 9, 7, 5, 10, 6, 1 },
			{ 13, 0, 11, 7, 4, 9, 1, 10, 14, 3, 5, 12, 2, 15, 8, 6 }, { 1, 4, 11, 13, 12, 3, 7, 14, 10, 15, 6, 8, 0, 5, 9, 2 },
			{ 6, 11, 13, 8, 1, 4, 10, 7, 9, 5, 0, 15, 14, 2, 3, 12 } };

	// int[4][16] S8
	public static int[][] S8 = { { 13, 2, 8, 4, 6, 15, 11, 1, 10, 9, 3, 14, 5, 0, 12, 7 },
			{ 1, 15, 13, 8, 10, 3, 7, 4, 12, 5, 6, 11, 0, 14, 9, 2 }, { 7, 11, 4, 1, 9, 12, 14, 2, 0, 6, 10, 13, 15, 3, 5, 8 },
			{ 2, 1, 14, 7, 4, 10, 8, 13, 15, 12, 9, 0, 3, 5, 6, 11 } };

	public static void main(String[] args) throws InterruptedException {

		DES2Kernel kernel = new DES2Kernel();
		StreamCompiler sc = new DebugStreamCompiler();
		CompiledStream<Integer, Integer> stream = sc.compile(kernel);
		Integer output;
		for (int i = 0; i < 100000;) {
			if (stream.offer(i)) {
				// System.out.println("Offer success " + i);
				i++;
			} else {
				// System.out.println("Offer failed " + i);
				Thread.sleep(10);
			}
			while ((output = stream.poll()) != null)
				System.out.println(output);
		}
		stream.drain();
		
		while (!stream.isDrained())
			while ((output = stream.poll()) != null)
				System.out.println(output);

		while ((output = stream.poll()) != null)
			System.out.println(output);
	}

	/**
	 * FIXME: Original implementations is "void->void pipeline DES2". Need to implement file, void supports.
	 */
	private static class DES2Kernel extends Pipeline<Integer, Integer> {
		int testvector = 7;

		DES2Kernel() {
			// add PlainTextSource(testvector);
			// add FileReader<int>("../input/input");
			add(new DEScoder(testvector));
			// add FileWriter<int>("des2.out");
			// add HexPrinter(CIPHERTEXT, 64);
		}
	}

	private static class DEScoder extends Pipeline<Integer, Integer> {
		DEScoder(int vector) {
			// initial permutation of 64 bit plain text
			add(new doIP());

			for (int i = 0; i < DES2.MAXROUNDS; i++) {

				Splitjoin<Integer, Integer> sp1 = new Splitjoin<>(new DuplicateSplitter<Integer>(), new RoundrobinJoiner<Integer>(32));

				// R[i+1] = f(R[i]) xor L[i]
				sp1.add(new nextR(vector, i));
				// L[i+1] = R[i]
				sp1.add(new nextL());
				add(sp1);

			}
			add(new CrissCross());

			add(new doIPm1());
		}
	}

	private static class doIP extends Filter<Integer, Integer> {
		doIP() {
			super(64, 64, 64); // TODO: Verify the peek value
		}

		public void work() {
			for (int i = 0; i < 64; i++) {
				push(peek(IP[i] - 1));
			}
			for (int i = 0; i < 64; i++) {
				pop();
			}
		}
	}

	// L[i+1] is lower 32 bits of current 64 bit input
	// input is LR[i]
	private static class nextL extends Filter<Integer, Integer> {
		nextL() {
			super(64, 32);
		}

		public void work() {
			for (int i = 0; i < 32; i++) {
				push(pop());
			}
			for (int i = 0; i < 32; i++) {
				pop(); // L[i] is decimated
			}
		}
	}

	// R[i+1] is f(R[i]) xor L[i]
	// R[i] is lower 32 bits of input stream
	// L[i] is upper 32 bits of input stream
	// input is LR[i]
	// output is f(R[i]) xor L[i]
	private static class nextR extends Pipeline<Integer, Integer> {
		nextR(int vector, int round) {
			add(new Splitjoin<Integer, Integer>(new RoundrobinSplitter<Integer>(32), new RoundrobinJoiner<Integer>(), new f(vector,
					round)), new Identity());
			add(new Xor(2));
		}
	}

	private static class f extends Pipeline<Integer, Integer> {
		f(int vector, int round) {
			// expand R from 32 to 48 bits and xor with key
			// add splitjoin {
			// split roundrobin(32, 0);
			add(new doE());
			add(new KeySchedule(vector, round));
			// join roundrobin;
			// }
			add(new Xor(2));

			// apply substitutions to generate 32 bit cipher
			add(new Sboxes());

			// permute the bits
			add(new doP());
		}
	}

	private static class doE extends Filter<Integer, Integer> {
		doE() {
			super(32, 48, 48); // TODO: Verify the peek value
		}

		public void work() {
			for (int i = 0; i < 48; i++) {
				push(peek(E[i] - 1));
			}
			for (int i = 0; i < 32; i++) {
				pop();
			}
		}
	}

	private static class doP extends Filter<Integer, Integer> {
		doP() {
			super(32, 32, 32); // TODO: Verify the peek value
		}

		public void work() {
			// input bit stream is from MSB ... LSB
			// that is LSB is head of FIFO, MSB is tail of FIFO
			// as in b63 b62 b61 b60 ... b3 b2 b1 b0
			// but P permutation requires bit numbering from left to right
			// as in b1 b2 b3 b4 ... b61 b62 b63 b64
			// (note indexing from 0 vs 1)
			// permutation P permutes the bits and emits them
			// in reverse order
			for (int i = 31; i >= 0; i--) {
				push(peek(32 - P[i]));
			}
			for (int i = 0; i < 32; i++) {
				pop();
			}
		}
	}

	private static class doIPm1 extends Filter<Integer, Integer> {
		doIPm1() {
			super(64, 64, 64); // TODO: Verify the peek value
		}

		public void work() {
			for (int i = 0; i < 64; i++) {
				push(peek(IPm1[i] - 1));
			}
			for (int i = 0; i < 64; i++) {
				pop();
			}
		}
	}

	/**
	 * This filter represents the first anonymous filter exists inside "int->int pipeline KeySchedule"
	 */
	private static class KeyScheduleFilter1 extends Filter<Integer, Integer> {
		int[][] keys;
		int vector;
		int round;

		KeyScheduleFilter1(int vector, int round) {
			super(48, 96);
			this.vector = vector;
			this.round = round;
			keys = new int[DES2.MAXROUNDS][48];
			init();
		}

		// precalculate key schedule
		private void init() {
			int[] k64 = new int[64];

			for (int w = 1; w >= 0; w--) {
				int v = USERKEYS[vector][w]; // LSW first then MSW
				int m = 1;
				for (int i = 0; i < 32; i++) {
					if (((v & m) >> i) != 0)
						k64[((1 - w) * 32) + i] = 1;
					else
						k64[((1 - w) * 32) + i] = 0;
					m = m << 1;
				}
			}

			// apply PC1
			int[] k56 = new int[56];
			for (int i = 0; i < 56; i++) {
				// input bit stream is from MSB ... LSB
				// that is LSB is head of FIFO, MSB is tail of FIFO
				// as in b63 b62 b61 b60 ... b3 b2 b1 b0
				// but PC1 permutation requires bit numbering from left to right
				// as in b1 b2 b3 b4 ... b61 b62 b63 b64
				// (note indexing from 0 vs 1)
				k56[i] = k64[64 - PC1[i]];
			}

			for (int r = 0; r < MAXROUNDS; r++) {
				// rotate left and right 28-bit bits chunks
				// according to round number
				int[] bits = new int[56];
				for (int i = 0; i < 28; i++)
					bits[i] = k56[(i + RT[r]) % 28];
				for (int i = 28; i < 56; i++)
					bits[i] = k56[28 + ((i + RT[r]) % 28)];
				for (int i = 0; i < 56; i++)
					k56[i] = bits[i];

				// apply PC2 and store resultant key
				for (int i = 47; i >= 0; i--) {
					// input bit stream is from MSB ... LSB
					// that is LSB is head of FIFO, MSB is tail of FIFO
					// as in b63 b62 b61 b60 ... b3 b2 b1 b0
					// permutation PC2 permutes the bits then emits them
					// in reverse order
					keys[r][47 - i] = k56[PC2[i] - 1];
				}
			}
		}

		public void work() {
			for (int i = 0; i < 48; i++) {
				push(keys[round][i]);
				push(pop());
			}
		}
	}

	/**
	 * This filter represents the second anonymous filter exists inside "int->int pipeline KeySchedule"
	 */
	private static class KeyScheduleFilter2 extends Filter<Integer, Integer> {
		int vector;
		int pushRate;
		int popRate;

		KeyScheduleFilter2(int popRate, int pushRate, int vector) {
			super(popRate, pushRate);
			this.vector = vector;
			this.pushRate = pushRate;
			this.popRate = popRate;
		}

		public void work() {
			for (int i = 0; i < popRate; i++)
				pop();

			push(USERKEYS[vector][1]); // LSW
			push(USERKEYS[vector][0]); // MSW
		}
	}

	/**
	 * This filter represents the first anonymous pipeline exists inside "int->int pipeline KeySchedule"
	 */
	private static class KeySchedulePipeline1 extends Pipeline<Integer, Integer> {
		KeySchedulePipeline1(int popRate, int pushRate, int vector) {
			add(new KeyScheduleFilter2(popRate, pushRate, vector));
			add(new IntoBits());
			add(new HexPrinter(USERKEY, 64));
		}
	}

	private static class KeySchedule extends Pipeline<Integer, Integer> {
		KeySchedule(int vector, int round) {
			add(new KeyScheduleFilter1(vector, round));

			if (PRINTINFO && (round == 0)) {
				// FIXME: join roundrobin(1, 0);
				add(new Splitjoin<Integer, Integer>(new DuplicateSplitter<Integer>(), new RoundrobinJoiner<Integer>(), new Identity(),
						new KeySchedulePipeline1(96, 2, vector)));
			}
		}
	}

	private static class Identity extends Filter<Integer, Integer> {
		Identity() {
			super(1, 1);
		}

		public void work() {
			push(pop());
		}
	}

	/**
	 * This filter represents the first anonymous filter exists inside "void->int pipeline slowKeySchedule"
	 */
	private static class slowKeyScheduleFilter1 extends Filter<Void, Integer> {
		int vector;

		public slowKeyScheduleFilter1(int vector) {
			super(0, 2);
			this.vector = vector;
		}

		public void work() {
			push(USERKEYS[vector][1]); // LSW
			push(USERKEYS[vector][0]); // MSW
		}

	}

	// inefficient but straightforward implementation of key schedule; it
	// recalculates all keys for all previous rounds 1...i-1
	private static class slowKeySchedule extends Pipeline<Void, Integer> {

		slowKeySchedule(int vector, int round) {
			add(new slowKeyScheduleFilter1(vector));
			add(new IntoBits());
			add(new doPC1());

			for (int i = 0; i < round + 1; i++) {
				add(new Splitjoin<Integer, Integer>(new RoundrobinSplitter<Integer>(28), new RoundrobinJoiner<Integer>(28),
						new LRotate(i), new LRotate(i)));
			}
			// or more simply can do:
			// add LRotate(i);
			add(new doPC2());
			if (PRINTINFO && (round == 0)) {
				// FIXME: join roundrobin(1, 0);
				add(new Splitjoin<Integer, Integer>(new DuplicateSplitter<Integer>(), new RoundrobinJoiner<Integer>(), new Identity(),
						new KeySchedulePipeline1(48, 2, vector)));
			}
		}
	}

	// left rotate input stream of length 28-bits by RT[round]
	private static class LRotate extends Filter<Integer, Integer> {
		int round;
		static int n = 28;
		int x;

		LRotate(int round) {
			super(n, n, n);
			this.round = round;
			x = RT[round];
		}

		public void work() {
			for (int i = 0; i < n; i++) {
				push(peek((i + x) % n));
			}
			for (int i = 0; i < n; i++) {
				pop();
			}
		}
	}

	private static class doPC1 extends Filter<Integer, Integer> {
		public doPC1() {
			super(64, 56, 64); // TODO: Verify the peek value
		}

		public void work() {
			for (int i = 0; i < 56; i++) {
				// input bit stream is from MSB ... LSB
				// that is LSB is head of FIFO, MSB is tail of FIFO
				// as in b63 b62 b61 b60 ... b3 b2 b1 b0
				// but PC1 permutation requires bit numbering from left to right
				// as in b1 b2 b3 b4 ... b61 b62 b63 b64
				// (note indexing from 0 vs 1)
				push(peek(64 - PC1[i]));
			}
			for (int i = 0; i < 64; i++) {
				pop();
			}
		}
	}

	private static class doPC2 extends Filter<Integer, Integer> {
		public doPC2() {
			super(56, 48, 48); // TODO: Verify the peek value
		}

		public void work() {
			// input bit stream is from MSB ... LSB
			// that is LSB is head of FIFO, MSB is tail of FIFO
			// as in b63 b62 b61 b60 ... b3 b2 b1 b0
			// permutation PC2 permutes the bits then emits them
			// in reverse order
			for (int i = 47; i >= 0; i--) {
				push(peek(PC2[i] - 1));
			}
			for (int i = 0; i < 56; i++) {
				pop();
			}
		}
	}

	private static class Sboxes extends Filter<Integer, Integer> {
		public Sboxes() {
			super(6 * 8, 4 * 8);
		}

		public void work() {
			for (int i = 1; i <= 8; i++) {
				int r = pop(); // r = first and last bit
				int c = pop(); // c = middle four bits
				c = (pop() << 1) | c;
				c = (pop() << 2) | c;
				c = (pop() << 3) | c;
				r = (pop() << 1) | r;

				int out = 0;
				if (i == 1)
					out = S8[r][c]; // lower 8 bits
				else if (i == 2)
					out = S7[r][c]; // next 8 bits
				else if (i == 3)
					out = S6[r][c]; // ...
				else if (i == 4)
					out = S5[r][c];
				else if (i == 5)
					out = S4[r][c];
				else if (i == 6)
					out = S3[r][c];
				else if (i == 7)
					out = S2[r][c];
				else if (i == 8)
					out = S1[r][c]; // last (upper) 8 bits

				push((int) ((out & 0x1) >> 0));
				push((int) ((out & 0x2) >> 1));
				push((int) ((out & 0x4) >> 2));
				push((int) ((out & 0x8) >> 3));
			}
		}
	}

	private static class PlainTextSourceFilter1 extends Filter<Void, Integer> {
		int vector;
		// int[34][2] TEXT
		int[][] TEXT = { { 0x00000000, 0x00000000 }, // 0x0000000000000000
				{ 0xFFFFFFFF, 0xFFFFFFFF }, // 0xFFFFFFFFFFFFFFFF
				{ 0x10000000, 0x00000001 }, // 0x1000000000000001
				{ 0x11111111, 0x11111111 }, // 0x1111111111111111
				{ 0x11111111, 0x11111111 }, // 0x1111111111111111
				{ 0x01234567, 0x89ABCDEF }, // 0x0123456789ABCDEF
				{ 0x00000000, 0x00000000 }, // 0x0000000000000000
				{ 0x01234567, 0x89ABCDEF }, // 0x0123456789ABCDEF
				{ 0x01A1D6D0, 0x39776742 }, // 0x01A1D6D039776742
				{ 0x5CD54CA8, 0x3DEF57DA }, // 0x5CD54CA83DEF57DA
				{ 0x0248D438, 0x06F67172 }, // 0x0248D43806F67172
				{ 0x51454B58, 0x2DDF440A }, // 0x51454B582DDF440A
				{ 0x42FD4430, 0x59577FA2 }, // 0x42FD443059577FA2
				{ 0x059B5E08, 0x51CF143A }, // 0x059B5E0851CF143A
				{ 0x0756D8E0, 0x774761D2 }, // 0x0756D8E0774761D2
				{ 0x762514B8, 0x29BF486A }, // 0x762514B829BF486A
				{ 0x3BDD1190, 0x49372802 }, // 0x3BDD119049372802
				{ 0x26955F68, 0x35AF609A }, // 0x26955F6835AF609A
				{ 0x164D5E40, 0x4F275232 }, // 0x164D5E404F275232
				{ 0x6B056E18, 0x759F5CCA }, // 0x6B056E18759F5CCA
				{ 0x004BD6EF, 0x09176062 }, // 0x004BD6EF09176062
				{ 0x480D3900, 0x6EE762F2 }, // 0x480D39006EE762F2
				{ 0x437540C8, 0x698F3CFA }, // 0x437540C8698F3CFA
				{ 0x072D43A0, 0x77075292 }, // 0x072D43A077075292
				{ 0x02FE5577, 0x8117F12A }, // 0x02FE55778117F12A
				{ 0x1D9D5C50, 0x18F728C2 }, // 0x1D9D5C5018F728C2
				{ 0x30553228, 0x6D6F295A }, // 0x305532286D6F295A
				{ 0x01234567, 0x89ABCDEF }, // 0x0123456789ABCDEF
				{ 0x01234567, 0x89ABCDEF }, // 0x0123456789ABCDEF
				{ 0x01234567, 0x89ABCDEF }, // 0x0123456789ABCDEF
				{ 0xFFFFFFFF, 0xFFFFFFFF }, // 0xFFFFFFFFFFFFFFFF
				{ 0x00000000, 0x00000000 }, // 0x0000000000000000
				{ 0x00000000, 0x00000000 }, // 0x0000000000000000
				{ 0xFFFFFFFF, 0xFFFFFFFF } }; // 0xFFFFFFFFFFFFFFFF

		public PlainTextSourceFilter1(int vector) {
			super(0, 2);
			this.vector = vector;
		}

		public void work() {
			push(TEXT[vector][1]); // LSW
			push(TEXT[vector][0]); // MSW
		}
	}

	private static class PlainTextSource extends Pipeline<Void, Integer> {
		PlainTextSource(int vector) {
			add(new PlainTextSourceFilter1(vector));
			add(new IntoBits());
			if (PRINTINFO) {
				// FIXME join roundrobin(1, 0);
				add(new Splitjoin<Integer, Integer>(new DuplicateSplitter<Integer>(), new RoundrobinJoiner<Integer>()));
				add(new Identity());
				add(new HexPrinter(PLAINTEXT, 64));
			}
		}
	}

	// take N streams and Xor them together
	// the streams are assumed to be interleaved
	private static class Xor extends Filter<Integer, Integer> {
		int n;

		public Xor(int n) {
			super(n, 1);
			this.n = n;
		}

		public void work() {
			int x = pop();
			for (int i = 1; i < n; i++) {
				int y = pop();
				x = x ^ y;
			}
			push(x);
		}
	}

	// swap two input streams each of 32 bits
	private static class CrissCross extends Filter<Integer, Integer> {
		public CrissCross() {
			super(64, 64, 64);
		}

		public void work() {
			for (int i = 0; i < 32; i++) {
				push(peek(32 + i));
			}
			for (int i = 0; i < 32; i++) {
				push(pop());
			}
			for (int i = 0; i < 32; i++) {
				pop();
			}
		}
	}

	// input: integer
	// output: LSB first ... MSB last
	private static class IntoBits extends Filter<Integer, Integer> {
		public IntoBits() {
			super(1, 32);
		}

		public void work() {
			int v = pop();
			int m = 1;

			for (int i = 0; i < 32; i++) {
				if (((v & m) >> i) != 0)
					push(1);
				else
					push(0);
				m = m << 1;
			}
		}
	}

	// input: LSB first ... MSB last
	// output: integer
	private static class BitstoInts extends Filter<Integer, Integer> {
		int n = 28;

		BitstoInts(int n) {
			super(n, 1, n);
			this.n = n;
		}

		public void work() {
			int v = 0;
			for (int i = 0; i < n; i++) {
				v = v | (pop() << i);
			}
			push(v);
		}
	}

	// input: w words x b bits/word
	// output: bit i from all w words, followed by i+1 for all b bits
	private static class BitSlice extends Splitjoin<Integer, Integer> {
		int w;
		int b;

		BitSlice(int w, int b) {
			super(new RoundrobinSplitter<Integer>(), new RoundrobinJoiner<Integer>(w));
			this.w = w;
			this.b = b;
			for (int l = 0; l < b; l++) {
				add(new Identity());
			}
		}
	}

	private static class HexPrinterFilter1 extends Filter<Integer, Void> {
		int descriptor;
		int bytes;

		HexPrinterFilter1(int descriptor, int bytes) {
			super(bytes, 0, bytes);
			this.bytes = bytes;
			this.descriptor = descriptor;
		}

		public void work() {
			if (PRINTINFO) {
				if (descriptor == PLAINTEXT)
					System.out.print("P: ");
				else if (descriptor == USERKEY)
					System.out.print("K: ");
				else if (descriptor == CIPHERTEXT)
					System.out.print("C: ");
			}

			for (int i = bytes - 1; i >= 0; i--) {
				int v = peek(i);
				if (v < 10)
					System.out.print(v);
				else if (v == 10)
					System.out.print("A");
				else if (v == 11)
					System.out.print("B");
				else if (v == 12)
					System.out.print("C");
				else if (v == 13)
					System.out.print("D");
				else if (v == 14)
					System.out.print("E");
				else if (v == 15)
					System.out.print("F");
				else {
					System.out.print("ERROR: ");
					System.out.println(v);
				}
			}
			System.out.println("");

			for (int i = 0; i < bytes; i++)
				pop();
		}

	}

	// input: LSB first ... MSB last
	// output: none
	// prints: MSW first ... LSW last
	private static class HexPrinter extends Pipeline<Integer, Void> {
		HexPrinter(int descriptor, int n) {
			int bits = n;
			int bytes = bits / 4;
			add(new BitstoInts(4));
			add(new HexPrinter(descriptor, bytes));
		}
	}

	/**
	 * This represents the anonymous fiter that exixtes inside "int->int splitjoin ShowIntermediate(int n)".
	 */
	private static class ShowIntermediateFilter1 extends Filter<Integer, Void> {
		int bytes;

		public ShowIntermediateFilter1(int bytes) {
			super(bytes, 0, bytes);
			this.bytes = bytes;
		}

		public void work() {
			for (int i = bytes - 1; i >= 0; i--) {
				int v = peek(i);
				if (v < 10)
					System.out.print(v);
				else if (v == 10)
					System.out.print("A");
				else if (v == 11)
					System.out.print("B");
				else if (v == 12)
					System.out.print("C");
				else if (v == 13)
					System.out.print("D");
				else if (v == 14)
					System.out.print("E");
				else if (v == 15)
					System.out.print("F");
				else {
					System.out.print("ERROR: ");
					System.out.println(v);
				}
			}
			System.out.println("");

			for (int i = 0; i < bytes; i++)
				pop();
		}
	}

	private static class ShowIntermediatePipeline1 extends Pipeline<Integer, Void> {
		ShowIntermediatePipeline1(int bytes) {
			add(new BitstoInts(4));
			add(new ShowIntermediateFilter1(bytes));
		}
	}

	// input: LSB first ... MSB last
	// output: LSB first ... MSB last (Identity)
	// prints: MSW first ... LSW last (HEX format)
	private static class ShowIntermediate extends Splitjoin<Integer, Integer> {

		ShowIntermediate(int n) {
			// FIXME join roundrobin(1, 0);
			super(new DuplicateSplitter<Integer>(), new RoundrobinJoiner<Integer>());
			add(new Identity());
			// FIXME: Need to add this. This is not the join roundrobin(1, 0) issue. The issue here is as join roundrobin(1,
			// 0), the second filter's output type is void. But Joiner's input type is not void and it receives the input from the
			// first filter.
			// add(new ShowIntermediatePipeline1(n / 4));
		}
	}

	// input: LSB first ... MSB last
	// output: LSB first ... MSB last (Identity)
	// prints: MSB first ... LSB last (BINARY format)
	private static class ShowBitStream extends Filter<Integer, Integer> {
		int n;
		int w;

		ShowBitStream(int n, int w) {
			super(n, n, n);
			this.n = n;
			this.w = w;
		}

		public void work() {
			for (int i = n - 1; i >= 0; i--) {
				System.out.print(peek(i));
				if ((i % w) == 0)
					System.out.print(" ");
			}
			System.out.println("");
			for (int i = 0; i < n; i++)
				push(pop());
		}
	}
}