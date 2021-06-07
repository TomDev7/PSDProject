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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;


public class FraudDetector extends ProcessWindowFunction<Tuple2<Integer, Double>, Tuple7<Integer, Double, Double, Double, Double, Double, Double>, Integer, GlobalWindow> {

	private static final long serialVersionUID = 1L;


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

		System.out.println("window (" + values.size() + ") : " + values.toString());

		a = calculateAverage(values);
		b = calculateMedian(values);
		c = calculateQuantile(values, 0.1);	//?
		d = calculateD(values);
		e = calculateE(values, a);
		f = calculateF(values, a);

//		out.collect("Window: " + context.window() + "count: " + count);
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

		Collections.sort(values);

		int middle = values.size() / 2;
		if (values.size()%2 == 1) {
			return values.get(middle);
		} else {
			return (values.get(middle-1) + values.get(middle)) / 2.0f;
		}
	}

	Double calculateQuantile(ArrayList<Double> values, Double quantile) {

		Collections.sort(values);

		return values.get((int)(values.size() * (1 - quantile)));
	}

	Double calculateD(ArrayList<Double> values) {

		Collections.sort(values);

		int pos = (int) (values.size() * 0.1);
		Double sum = 0.0;

		for (int i = 0; i <= pos; i++) {
			sum += values.get(i);
		}

		return sum/pos;
	}

	Double calculateE(ArrayList<Double> values, Double average) {

		Double pSum = 0.0;

		for (Double v : values) {

			pSum += Math.abs(average - v);
		}

		return average - (1/(2*values.size())) * pSum;
	}

	Double calculateF(ArrayList<Double> values, Double average) {

		Double pSum = 0.0;

		for (Double v1 : values) {
			for (Double v2 : values) {
				pSum += Math.abs(v1 - v2);
			}
		}

		return average - (1/(2*values.size()^2));
	}
}
