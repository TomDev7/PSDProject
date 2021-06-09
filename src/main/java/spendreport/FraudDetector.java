/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spendreport;

import com.sun.org.apache.bcel.internal.generic.BIPUSH;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;

import static org.apache.flink.shaded.curator4.com.google.common.math.Quantiles.percentiles;


public class FraudDetector extends ProcessWindowFunction<Tuple2<Integer, Double>, Tuple7<Integer, Double, Double, Double, Double, Double, Double>, Integer, GlobalWindow> {

	private static final long serialVersionUID = 1L;
	private static int val0_window_counter = 0;
	private static int val1_window_counter = 0;
	private static int val2_window_counter = 0;
	private static int val3_window_counter = 0;
	private static int val4_window_counter = 0;
	private static int val5_window_counter = 0;
	private static int val6_window_counter = 0;	// portfel



	@Override
	public void open(Configuration parameters) {	//rozpoczęcie procesu przetwarzania


	}

	@Override
	public void process(Integer key, Context context, Iterable<Tuple2<Integer, Double>> input, Collector<Tuple7<Integer, Double, Double, Double, Double, Double, Double>> out) throws Exception {

		//System.out.println("process window: " + context.window());

		ArrayList<Double> values = new ArrayList<>();

		// Wartosci ponizej odpowiadaja nazwami podpunktom z zadania
		Double a = 0.0;	//srednia
		Double b = 0.0;	//mediana
		Double c = 0.0;	//kwantyl
		Double d = 0.0;	//srednia z 10%
		Double e = 0.0;	//miara bezpieczenstwa na odchyleniu
		Double f = 0.0;	//miara bezpieczenstwa na sredniej


		for (Tuple2<Integer, Double> in: input) {
			values.add(in.f1);
		}

		if (values.size() < 30) {
			return;
		}

		//System.out.println("window (size: " + values.size() + ") : " + values.toString());
		a = calculateAverage(values);
		b = calculateMedian(values);
		c = calculateQuantile(values, 10);	//?
		d = calculateAverageOfLower10Perc(values);
		e = calculateMB1(values, a);
		f = calculateMB2(values, a);

//		out.collect("Window: " + context.window() + "count: " + count);
		switch (key){
			case 0: {
				FraudDetector.val0_window_counter += 1;
				System.out.println("key: " + key + ", window no: " + val0_window_counter + ", a: " + a + ", b: " + b + ", c: " + c + ", d: " + d + ", e: " + e + ", f: " + f);
				break;
			}
			case 1: {
				FraudDetector.val1_window_counter += 1;
				System.out.println("key: " + key + ", window no: " + val1_window_counter + ", a: " + a + ", b: " + b + ", c: " + c + ", d: " + d + ", e: " + e + ", f: " + f);
				break;
			}
			case 2: {
				FraudDetector.val2_window_counter += 1;
				System.out.println("key: " + key + ", window no: " + val2_window_counter + ", a: " + a + ", b: " + b + ", c: " + c + ", d: " + d + ", e: " + e + ", f: " + f);
				break;
			}
			case 3: {
				FraudDetector.val3_window_counter += 1;
				System.out.println("key: " + key + ", window no: " + val3_window_counter + ", a: " + a + ", b: " + b + ", c: " + c + ", d: " + d + ", e: " + e + ", f: " + f);
				break;
			}
			case 4: {
				FraudDetector.val4_window_counter += 1;
				System.out.println("key: " + key + ", window no: " + val4_window_counter + ", a: " + a + ", b: " + b + ", c: " + c + ", d: " + d + ", e: " + e + ", f: " + f);
				break;
			}
			case 5: {
				FraudDetector.val5_window_counter += 1;
				System.out.println("key: " + key + ", window no: " + val5_window_counter + ", a: " + a + ", b: " + b + ", c: " + c + ", d: " + d + ", e: " + e + ", f: " + f);
				break;
			}
			case 6: {
				FraudDetector.val6_window_counter += 1;
				System.out.println("key: " + key + ", window no: " + val6_window_counter + ", a: " + a + ", b: " + b + ", c: " + c + ", d: " + d + ", e: " + e + ", f: " + f);
				break;
			}
		}

		// sprawdzanie warunkow alarmu
		if ((Config.A_for_all_data - a) / (1 + Config.A_for_all_data) >= Config.alert_threshold) {

		}
		if ((Config.B_for_all_data - b) / (1 + Config.B_for_all_data) >= Config.alert_threshold) {

		}
		if ((Config.C_for_all_data - c) / (1 + Config.C_for_all_data) >= Config.alert_threshold) {

		}
		if ((Config.D_for_all_data - d) / (1 + Config.D_for_all_data) >= Config.alert_threshold) {

		}
		if ((Config.E_for_all_data - e) / (1 + Config.E_for_all_data) >= Config.alert_threshold) {

		}
		if ((Config.F_for_all_data - f) / (1 + Config.F_for_all_data) >= Config.alert_threshold) {

		}

		out.collect(Tuple7.of(key, a, b, c, d, e, f));
	}

	Double calculateAverage(ArrayList<Double> values) {

		Double sum = 0.0;

		for (Double v : values) {

			sum += v;
		}

		return sum/values.size();
	}

	Double calculateMedian(ArrayList<Double> values) {

		ArrayList<Double> copy = new ArrayList<>();
		copy.addAll(values);
		Collections.sort(copy);

		int middle = copy.size() / 2;
		if (copy.size()%2 == 1) {
			return copy.get(middle);
		} else {
			return (copy.get(middle-1) + copy.get(middle)) / 2.0f;
		}
	}

	Double calculateQuantile(ArrayList<Double> values, int quantile) {

		double quantile10 = percentiles().index(quantile).compute(values);
		return quantile10;
	}

	Double calculateAverageOfLower10Perc(ArrayList<Double> values) {

		ArrayList<Double> copy = new ArrayList<>();
		copy.addAll(values);
		Collections.sort(copy);
		int lower10PercValuesIdx = copy.size() / 10;

		Double sum = 0.0;

		for (int i = 0; i < lower10PercValuesIdx; i++) {
			sum += copy.get(i);
		}

		return sum/lower10PercValuesIdx;
	}

	Double calculateMB1(ArrayList<Double> values, Double average) {

		BigDecimal pSum = new BigDecimal(0);
		BigDecimal val = null;
		BigDecimal abs = null;
		BigDecimal averageBD = new BigDecimal(average);
		for(Double v: values) {
			val = new BigDecimal(v);
			abs = averageBD.subtract(val).abs();
			pSum = pSum.add(abs);
		}

		BigDecimal result = averageBD.subtract(pSum.divide(BigDecimal.valueOf(2*values.size()), 10, RoundingMode.HALF_UP));
		return result.doubleValue();
	}

	Double calculateMB2(ArrayList<Double> values, Double average) {

		BigDecimal outterSum = new BigDecimal(0);
		BigDecimal innerSum = new BigDecimal(0);
		BigDecimal valOut = null;
		BigDecimal valIn = null;
		BigDecimal abs = null;
		BigDecimal averageBD = new BigDecimal(average);
		for(Double v: values) {
			valOut = new BigDecimal(v);
			innerSum = new BigDecimal(0);
			for(Double v2: values) {
				valIn = new BigDecimal(v2);
				abs = valOut.subtract(valIn).abs();
				innerSum = innerSum.add(abs);
			}
			outterSum = outterSum.add(innerSum);
		}

		BigDecimal result = averageBD.subtract(outterSum.divide(BigDecimal.valueOf(2*values.size()*values.size()), 10,
				RoundingMode.HALF_UP));
		return result.doubleValue();

	}
}
