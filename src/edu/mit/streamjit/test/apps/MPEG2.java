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
import edu.mit.streamjit.api.DuplicateSplitter;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StatefulFilter;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.WeightedRoundrobinJoiner;
import edu.mit.streamjit.api.WeightedRoundrobinSplitter;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmark.Dataset;
import edu.mit.streamjit.test.Benchmarker;
import edu.mit.streamjit.test.Datasets;
import edu.mit.streamjit.test.SuppliedBenchmark;
import edu.mit.streamjit.test.apps.dct.DCT2;
import java.nio.ByteOrder;
import java.nio.file.Paths;

/**
 * Ported from streamit/streams/apps/benchmarks/asplos06/mpeg2/streamit/MPEGdecoder_nomessage_5_3.str
 *
 * See comments on InverseQuantization.
 *
 * BestSaturation ought to be autotuned.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 2/8/2014
 */
public final class MPEG2 {
	private MPEG2() {}

	public static void main(String[] args) {
		StreamCompiler sc = new DebugStreamCompiler();
		Benchmarker.runBenchmark(new MPEG2Benchmark(), sc).get(0).print(System.out);
	}

	@ServiceProvider(Benchmark.class)
	public static final class MPEG2Benchmark extends SuppliedBenchmark {
		private static final int COPIES = 1;
		public MPEG2Benchmark() {
			super("MPEG2", MPEGdecoder_nomessage_5_3.class, new Dataset("dec_nm_parsed.int",
					(Input)Datasets.nCopies(COPIES, (Input)Input.fromBinaryFile(Paths.get("data/dec_nm_parsed.int"), Integer.class, ByteOrder.LITTLE_ENDIAN))
					//, (Supplier)Suppliers.ofInstance((Input)Input.fromBinaryFile(Paths.get("/home/jbosboom/streamit/streams/apps/benchmarks/asplos06/mpeg2/streamit/dec_nm_color_channels_input_3.int"), Integer.class, ByteOrder.LITTLE_ENDIAN))
			));
		}
	}

	// Index is the chroma format
    // as defined in the MPEG-2 bitstream
    // 1 = 4:2:0, 2 = 4:2:2, 3 = 4:4:4
	private static final int[] BLOCKS_PER_MACROBLOCK = {0, 6, 8, 12};
	public static final class MPEGdecoder_nomessage_5_3 extends Splitjoin<Integer, Integer> {
		private static final int WIDTH = 352, HEIGHT = 240, CHROMA_FORMAT = 1;
		public MPEGdecoder_nomessage_5_3() {
			super(new WeightedRoundrobinSplitter<Integer>(64 * BLOCKS_PER_MACROBLOCK[CHROMA_FORMAT], 16, 3),
					new WeightedRoundrobinJoiner<Integer>(64, 8, 3),
					new BlockDecode(),
					new Pipeline<Integer, Integer>(
							new MotionVectorDecode(),
							new Repeat(8, BLOCKS_PER_MACROBLOCK[CHROMA_FORMAT])
					),
					new Repeat(3, BLOCKS_PER_MACROBLOCK[CHROMA_FORMAT]) // macroblock_intra
			);
		}
	}

	private static final class BlockDecode extends Pipeline<Integer, Integer> {
		private BlockDecode() {
			super(new ZigZagUnordering(),
					new InverseQuantization(),
					new BestSaturation(-2048, 2047, -2050, 2050),
					new MismatchControl(),
					new DCT2.iDCT8x8_2D_fast_fine(),
					new BestSaturation(-256, 255, -260, 260));
		}
	}

	/**
	 * In the StreamIt source this filter uses an array named peekSubstitute,
	 * with no reason given.
	 */
	private static final class ZigZagUnordering extends Filter<Integer, Integer> {
		private static final int[] ORDERING =
						{0, 1, 5, 6, 14, 15, 27, 28,
                        2, 4, 7, 13, 16, 26, 29, 42,
                        3, 8, 12, 17, 25, 30, 41, 43,
                        9, 11, 18, 24, 31, 40, 44, 53,
                        10, 19, 23, 32, 39, 45, 52, 54,
                        20, 22, 33, 38, 46, 51, 55, 60,
                        21, 34, 37, 47, 50, 56, 59, 61,
                        35, 36, 48, 49, 57, 58, 62, 63};
		private ZigZagUnordering() {
			super(64, 64, 64);
		}
		@Override
		public void work() {
			for (int i = 0; i < 64; ++i)
				push(peek(ORDERING[i]));
			for (int i = 0; i < 64; ++i)
				pop();
		}
	}

	/**
	 * In the StreamIt source, this pipeline contains a splitjoin for
	 * intra/non-intra coded macroblocks, then a
	 * InverseQuantizationJoinerSubstitute filter that throws away the result of
	 * one branch.  However in this specific benchmark, the substitute always
	 * assumes it's an intra-coded macroblock (macroblock_intra = 1 in init and
	 * no other assignment),
	 */
	private static final class InverseQuantization extends Pipeline<Integer, Integer> {
		private InverseQuantization() {
			super(new Splitjoin<>(new DuplicateSplitter<Integer>(), new RoundrobinJoiner<Integer>(64),
						// Intra Coded Macroblocks
						new Splitjoin<>(new WeightedRoundrobinSplitter<Integer>(1, 63), new WeightedRoundrobinJoiner<Integer>(1, 63),
								new InverseQuantization_DC_Intra_Coeff(),
								new InverseQuantization_AC_Coeff(1)
						),
						// Non Intra Coded Macroblocks
						new InverseQuantization_AC_Coeff(0)
					),
					// Selects which stream - FEATURETODO eventually programmable splitjoin and only one of the two
					// above branches gets taken instead of both.
					new InverseQuantizationJoinerSubstitute()
			);
		}
	}

	private static final class InverseQuantizationJoinerSubstitute extends Filter<Integer, Integer> {
		private static final int macroblock_intra = 1;
		private InverseQuantizationJoinerSubstitute() {
			super(128, 64);
		}
		@Override
		public void work() {
			if (macroblock_intra == -1) {
				System.out.println("  Error: macroblock_intra should not be -1, should have recieved update message");
			} else if (macroblock_intra == 1) {
				// It was Intra Coded
				for (int i = 0; i < 64; i++) {
					push(pop());
				}
				for (int i = 0; i < 64; i++) {
					pop();
				}
			} else {
				// It was Non Intra Coded
				for (int i = 0; i < 64; i++) {
					pop();
				}
				for (int i = 0; i < 64; i++) {
					push(pop());
				}
			}
		}
	}

	private static final class InverseQuantization_DC_Intra_Coeff extends Filter<Integer, Integer> {
		private static final int[] INTRA_DC_MULT = {8, 4, 2, 1};
		private static final int INTRA_DC_PRECISION = 0;
		private InverseQuantization_DC_Intra_Coeff() {
			super(1, 1);
		}
		@Override
		public void work() {
			push(INTRA_DC_MULT[INTRA_DC_PRECISION] * pop());
		}
	}

	private static final class InverseQuantization_AC_Coeff extends Filter<Integer, Integer> {
		// Assumes 4:2:0 data
		// (cite 1, P.69)
		// intra = 1: This is dequantizing the non-DC part of an intra coded block
		// intra = 0: This is dequantizing the DC and AC part of a non-intra coded block
		// These are all assigned by messages and MUST be assigned before the first
		// call to work()
		private static final int quantiser_scale_code = 1;
		private static final int q_scale_type = 0;
		private static final int[] intra_quantiser_matrix = { 8, 16, 19, 22, 26, 27, 29, 34,
										   16, 16, 22, 24, 27, 29, 34, 37,
										   19, 22, 26, 27, 29, 34, 34, 38,
										   22, 22, 26, 27, 29, 34, 37, 40,
										   22, 26, 27, 29, 32, 35, 40, 48,
										   26, 27, 29, 32, 35, 40, 48, 58,
										   26, 27, 29, 34, 38, 46, 56, 69,
										   27, 29, 35, 38, 46, 56, 69, 83};
		private static final int[] non_intra_quantiser_matrix = {16, 16, 16, 16, 16, 16, 16, 16,
											  16, 16, 16, 16, 16, 16, 16, 16,
											  16, 16, 16, 16, 16, 16, 16, 16,
											  16, 16, 16, 16, 16, 16, 16, 16,
											  16, 16, 16, 16, 16, 16, 16, 16,
											  16, 16, 16, 16, 16, 16, 16, 16,
											  16, 16, 16, 16, 16, 16, 16, 16,
											  16, 16, 16, 16, 16, 16, 16, 16};
		// (cite 1, P.70 Table 7-6)
		private static final int[][] quantiser_scale =
			// Note that quantiser_scale[x][0] is a Forbidden Value
			{{ 0,  2,  4,  6,  8, 10, 12, 14,
			   16, 18, 20, 22, 24, 26, 28, 30,
			   32, 34, 36, 38, 40, 42, 44, 46,
			   48, 50, 52, 54, 56, 58, 60, 62},
			 { 0,  1,  2,  3,  4,  5,  6,  7,
			   8, 10, 12, 14, 16, 18, 20, 22,
			   24, 28, 32, 36, 40, 44, 48, 52,
			   56, 64, 72, 80, 88, 96, 104, 112}};
		private final int macroblock_intra;
		private InverseQuantization_AC_Coeff(int macroblock_intra) {
			super(64-macroblock_intra, 64-macroblock_intra);
			this.macroblock_intra = macroblock_intra;
		}
		@Override
		public void work() {
			if (quantiser_scale_code == 0)
				System.out.println("Error - quantiser_scale_code not allowed to be 0 " + macroblock_intra);
			for (int i = macroblock_intra; i < 64; i++) {
				int QF = pop();
				// (cite 1, P.71)
				int k = 0;
				if (macroblock_intra == 1) {
					k = 0;
				} else {
					// TODO - I think I'm interpreting this part of the spec correctly, check though.
					if (QF > 0) {
						k = 1;
					} else if (QF < 0) {
						k = -1;
					} else {
						k = 0;
					}
				}
				int W = 0;
				if (macroblock_intra == 1) {
					W = intra_quantiser_matrix[i];
				} else {
					W = non_intra_quantiser_matrix[i];
				}
				int F = (2 * QF + k) * W *
					quantiser_scale[q_scale_type][quantiser_scale_code] / 32;
				push(F);
			}
		}
	}

	private static final class BestSaturation extends Pipeline<Integer, Integer> {
		private BestSaturation(int min, int max, int worst_input_min, int worst_input_max) {
			super((worst_input_max - worst_input_min + 1) < 600 ?
					new BoundedSaturation(min, max, worst_input_min, worst_input_max) :
					new Saturation(min, max));
		}
	}

	private static final class Saturation extends Filter<Integer, Integer> {
		private final int min, max;
		private Saturation(int min, int max) {
			super(1, 1);
			this.min = min;
			this.max = max;
		}
		@Override
		public void work() {
			int val = pop();
			if (val > max)
				push(max);
			else if (val < min)
				push(min);
			else
				push(val);
		}
	}

	private static final class BoundedSaturation extends Filter<Integer, Integer> {
		private final int[] lookupTable;
		private final int worst_input_min;
		private BoundedSaturation(int min, int max, int worst_input_min, int worst_input_max) {
			super(1, 1);
			this.worst_input_min = worst_input_min;
			this.lookupTable = new int[worst_input_max - worst_input_min + 1];
			for (int i = worst_input_min; i <= worst_input_max; ++i) {
				if (i > max)
					lookupTable[i - worst_input_min] = max;
				else if (i < min)
					lookupTable[i - worst_input_min] = min;
				else
					lookupTable[i - worst_input_min] = i;
			}
		}
		@Override
		public void work() {
			push(lookupTable[pop() - worst_input_min]);
		}
	}

	private static final class MismatchControl extends Filter<Integer, Integer> {
		private MismatchControl() {
			super(64, 64);
		}
		@Override
		public void work() {
			int sum, val;
			sum = 0;
			for (int i = 0; i < 63; i++) {
				val = pop();
				sum += val;
				push(val);
			}
			val = pop();
			sum += val;
			if ((sum & 0x1) == 0x1) {
				push(val);
			} else {
				if ((val * 0x1) == 0x1) {
					push(val-1);
				} else {
					push(val+1);
				}
			}
		}
	}

	private static final class MotionVectorDecode extends StatefulFilter<Integer, Integer> {
		// Note - at first glance, this filter looks like it OUGHT to handle. only a single motion vector instead
		// of all 8, and then it would be wrapped inside an 8 way splitjoin. This is only because of currently
		// existing limitations in this code, however. More general MPEG-2 bitstreams allow for concealment
		// motion vectors (to help in the case of errors introduced during transmission of the bitstream), and
		// when concealment motion vectors are introduced, then dependencies are introduced between the
		// vectors. These dependencies will make it hard to use an 8-way splitjoin approach without a
		// message passing scheme that allows for across splitjoin messaging.

		// Section 7.6.3.1 covers this. (cite 1, P.77)
		private final int[][][] PMV = new int[2][2][2];
		private final int mv_format = 1; // HACKED TODO - MESSAGING
		private final int picture_structure = 1; // HACKED TODO - MESSAGING
		private MotionVectorDecode() {
			super(16, 8);
		}
		@Override
		public void work() {
			int[][][] motion_code = new int[2][2][2];
			for (int r = 0; r < 2; r++)
				for (int s = 0; s < 2; s++)
					for (int t = 0; t < 2; t++) {
						motion_code[r][s][t] = pop();
					}
			int[][][] motion_residual = new int[2][2][2];
			for (int r = 0; r < 2; r++)
				for (int s = 0; s < 2; s++)
					for (int t = 0; t < 2; t++) {
						motion_residual[r][s][t] = pop();
					}
			int[][][] vectorp = new int[2][2][2];
			for (int r = 0; r < 1; r++) {
				// NOTE TODO - Hacked right now, don't know when we need the second motion vector.
				for (int s = 0; s < 2; s++) {
					for (int t = 0; t < 2; t++) {
						int r_size = 14;
						int f = 1 << r_size;
						int high = (16*f)-1;
						int low = ((-16)*f);
						int range = (32*f);
						int delta;
						if ((f == 1) || (motion_code[r][s][t] == 0)) {
							delta = motion_code[r][s][t];
						} else {
							delta = ((int) (Math.abs(motion_code[r][s][t])-1)*f) +
								motion_residual[r][s][t]+1;
							if (motion_code[r][s][t]<0)
								delta = -delta;
						}
						int prediction = PMV[r][s][t];
						if ((mv_format == 0) && (t == 1) && (picture_structure == 3))
							System.out.println("Error - Program Limitation: May not be correct in decoding motion vectors");
						vectorp[r][s][t] = prediction + delta;
						if (vectorp[r][s][t] < low)
							vectorp[r][s][t] = vectorp[r][s][t] + range;
						if (vectorp[r][s][t] > high)
							vectorp[r][s][t] = vectorp[r][s][t] - range;
						if ((mv_format == 0) && (t == 1) && (picture_structure == 3))
							System.out.println("Error - Program Limitation: May not be correct in decoding motion vectors");
						else
							PMV[r][s][t] = vectorp[r][s][t];
						// TODO handle updating missed motion_vectors
						// section 7.6.3.3
					}
				}
			}
			for (int r = 0; r < 2; r++)
				for (int s = 0; s < 2; s++)
					for (int t = 0; t < 2; t++) {
						push(vectorp[r][s][t]);
					}
		}
	}

	private static final class Repeat extends Filter<Integer, Integer> {
		private final int numitems, numtimes;
		private Repeat(int numitems, int numtimes) {
			super(numitems, numitems * numtimes, numitems);
			this.numitems = numitems;
			this.numtimes = numtimes;
		}
		@Override
		public void work() {
			for (int i = 0; i < numtimes; ++i)
				for (int j = 0; j < numitems; ++j)
					push(peek(j));
			for (int i = 0; i < numitems; ++i)
				pop();
		}
	}
}
