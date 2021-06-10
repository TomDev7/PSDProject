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

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
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

	private static int filenameCounters[] = {0,0,0,0,0,0,0};



	@Override
	public void open(Configuration parameters) {	//rozpoczęcie procesu przetwarzania


	}

	@Override
	public void process(Integer key, Context context, Iterable<Tuple2<Integer, Double>> input, Collector<Tuple7<Integer, Double, Double, Double, Double, Double, Double>> out) throws Exception {

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

		a = calculateAverage(values);
		b = calculateMedian(values);
		c = calculateQuantile(values, 10);	//?
		d = calculateAverageOfLower10Perc(values);
		e = calculateMB1(values, a);
		f = calculateMB2(values, a);

		int currentWindow = -1;

		switch (key){
			case 0: {
				FraudDetector.val0_window_counter += 1;
				currentWindow = val0_window_counter;
				//System.out.println("key: " + key + ", window no: " + val0_window_counter + ", a: " + a + ", b: " + b + ", c: " + c + ", d: " + d + ", e: " + e + ", f: " + f);
				break;
			}
			case 1: {
				FraudDetector.val1_window_counter += 1;
				currentWindow = val1_window_counter;
				//System.out.println("key: " + key + ", window no: " + val1_window_counter + ", a: " + a + ", b: " + b + ", c: " + c + ", d: " + d + ", e: " + e + ", f: " + f);
				break;
			}
			case 2: {
				FraudDetector.val2_window_counter += 1;
				currentWindow = val2_window_counter;
				//System.out.println("key: " + key + ", window no: " + val2_window_counter + ", a: " + a + ", b: " + b + ", c: " + c + ", d: " + d + ", e: " + e + ", f: " + f);
				break;
			}
			case 3: {
				FraudDetector.val3_window_counter += 1;
				currentWindow = val3_window_counter;
				//System.out.println("key: " + key + ", window no: " + val3_window_counter + ", a: " + a + ", b: " + b + ", c: " + c + ", d: " + d + ", e: " + e + ", f: " + f);
				break;
			}
			case 4: {
				FraudDetector.val4_window_counter += 1;
				currentWindow = val4_window_counter;
				//System.out.println("key: " + key + ", window no: " + val4_window_counter + ", a: " + a + ", b: " + b + ", c: " + c + ", d: " + d + ", e: " + e + ", f: " + f);
				break;
			}
			case 5: {
				FraudDetector.val5_window_counter += 1;
				currentWindow = val5_window_counter;
				//System.out.println("key: " + key + ", window no: " + val5_window_counter + ", a: " + a + ", b: " + b + ", c: " + c + ", d: " + d + ", e: " + e + ", f: " + f);
				break;
			}
			case 6: {
				FraudDetector.val6_window_counter += 1;
				currentWindow = val6_window_counter;
				//System.out.println("key: " + key + ", window no: " + val6_window_counter + ", a: " + a + ", b: " + b + ", c: " + c + ", d: " + d + ", e: " + e + ", f: " + f);
				break;
			}
		}

		if (currentWindow % 1000 == 0) {
			System.out.println("processing window: " + currentWindow);
		}

		// sprawdzanie warunkow alarmu
		Double a_compare_result = (Config.getInstance().getMean(key) - a) / (1 + Config.getInstance().getMean(key));
		Double b_compare_result = (Config.getInstance().getMedian(key) - b) / (1 + Config.getInstance().getMedian(key));
		Double c_compare_result  = (Config.getInstance().getQuantile(key) - c) / (1 + Config.getInstance().getQuantile(key));
		Double d_compare_result = (Config.getInstance().getMean10Lowest(key) - d) / (1 + Config.getInstance().getMean10Lowest(key));
		Double e_compare_result = (Config.getInstance().getMb1(key) - e) / (1 + Config.getInstance().getMb1(key));
		Double f_compare_result = (Config.getInstance().getMb2(key) - f) / (1 + Config.getInstance().getMb2(key));

		if ( a_compare_result >= Config.getInstance().getAlertThreshold()) {
			String reportToStdout = "Mean value exceeded alert threshold. Window: " + currentWindow + ", asset: " + key + ", value: " + a_compare_result;
			String reportToFile = "Mean" +";"+ currentWindow +";"+ key +";"+ a_compare_result;
			System.out.println(reportToStdout);
			printToFile(reportToFile, key);
		}
		if (b_compare_result >= Config.getInstance().getAlertThreshold()) {
			String reportToStdout = "Median value exceeded alert threshold. Window: " + currentWindow + ", asset: " + key + ", value: " + b_compare_result;
			String reportToFile = "Median" +";"+ currentWindow +";"+ key +";"+ b_compare_result;
			System.out.println(reportToStdout);
			printToFile(reportToFile, key);
		}
		if ( c_compare_result >= Config.getInstance().getAlertThreshold()) {
			String reportToStdout ="Quantile value exceeded alert threshold. Window: " + currentWindow + ", asset: " + key + ", value: " + c_compare_result;
			String reportToFile = "Quantile" +";"+ currentWindow +";"+ key +";"+ c_compare_result;
			System.out.println(reportToStdout);
			printToFile(reportToFile, key);
		}
		if (d_compare_result >= Config.getInstance().getAlertThreshold()) {
			String reportToStdout = "Mean10Lowest value exceeded alert threshold. Window: " + currentWindow + ", asset: " + key + ", value: " + d_compare_result;
			String reportToFile = "Mean10Lowest" +";"+ currentWindow +";"+ key +";"+ d_compare_result;
			System.out.println(reportToStdout);
			printToFile(reportToFile, key);
		}
		if (e_compare_result >= Config.getInstance().getAlertThreshold()) {
			String reportToStdout = "Mb1 value exceeded alert threshold. Window: " + currentWindow + ", asset: " + key + ", value: " + e_compare_result;
			String reportToFile = "Mb1" +";"+ currentWindow +";"+ key +";"+ e_compare_result;
			System.out.println(reportToStdout);
			printToFile(reportToFile, key);
		}
		if (f_compare_result >= Config.getInstance().getAlertThreshold()) {
			String reportToStdout = "Mb2 value exceeded alert threshold. Window: " + currentWindow + ", asset: " + key + ", value: " + f_compare_result;
			String reportToFile = "Mb2" +";"+ currentWindow +";"+ key +";"+ f_compare_result;
			System.out.println(reportToStdout);
			printToFile(reportToFile, key);
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

	int printToFile(String text, int key) throws Exception {

		String fileName = "default_reports.txt";
		switch (key){
			case 0: {
				fileName = "val0_reports_" + filenameCounters[0] + ".csv";
				File file = new File("/home/george/Pulpit/Projekt PSD/" + fileName);
				if (file.exists() && file.isFile() && file.length() > Config.getInstance().getFileSizeLimit()){
					filenameCounters[0]++;
					fileName = "val0_reports_" + filenameCounters[0] + ".csv";
				}
				break;
			}
			case 1: {
				fileName = "val1_reports_" + filenameCounters[1] + ".csv";
				File file = new File("/home/george/Pulpit/Projekt PSD/" + fileName);
				if (file.exists() && file.isFile() && file.length() > Config.getInstance().getFileSizeLimit()){
					filenameCounters[1]++;
					fileName = "val1_reports_" + filenameCounters[1] + ".csv";
				}
				break;
			}
			case 2: {
				fileName = "val2_reports_" + filenameCounters[2] + ".csv";
				File file = new File("/home/george/Pulpit/Projekt PSD/" + fileName);
				if (file.exists() && file.isFile() && file.length() > Config.getInstance().getFileSizeLimit()){
					filenameCounters[2]++;
					fileName = "val2_reports_" + filenameCounters[2] + ".csv";
				}
				break;
			}
			case 3: {
				fileName = "val3_reports_" + filenameCounters[3] + ".csv";
				File file = new File("/home/george/Pulpit/Projekt PSD/" + fileName);
				if (file.exists() && file.isFile() && file.length() > Config.getInstance().getFileSizeLimit()){
					filenameCounters[3]++;
					fileName = "val3_reports_" + filenameCounters[3] + ".csv";
				}
				break;
			}
			case 4: {
				fileName = "val4_reports_" + filenameCounters[4] + ".csv";
				File file = new File("/home/george/Pulpit/Projekt PSD/" + fileName);
				if (file.exists() && file.isFile() && file.length() > Config.getInstance().getFileSizeLimit()){
					filenameCounters[4]++;
					fileName = "val4_reports_" + filenameCounters[4] + ".csv";
				}
				break;
			}
			case 5: {
				fileName = "val5_reports_" + filenameCounters[5] + ".csv";
				File file = new File("/home/george/Pulpit/Projekt PSD/" + fileName);
				if (file.exists() && file.isFile() && file.length() > Config.getInstance().getFileSizeLimit()){
					filenameCounters[5]++;
					fileName = "val5_reports_" + filenameCounters[5] + ".csv";
				}
				break;
			}
			case 6: {
				fileName = "val6_reports_" + filenameCounters[6] + ".csv";
				File file = new File("/home/george/Pulpit/Projekt PSD/" + fileName);
				if (file.exists() && file.isFile() && file.length() > Config.getInstance().getFileSizeLimit()){
					filenameCounters[6]++;
					fileName = "val6_reports_" + filenameCounters[6] + ".csv";
				}
				break;
			}
		}

		String filePath = "/home/george/Pulpit/Projekt PSD/" + fileName;
		FileWriter fileWriter = new FileWriter(filePath, true);
		PrintWriter printWriter = new PrintWriter(fileWriter, true);
		printWriter.println(text);
		printWriter.close();

		/*
		* format pliku csv:
		* String: statystyka ; Integer: window (ktore okno) ; Integer: key (ktore aktywo) ; Double: wynik porownania z progiem alertu
		* */

		return 0;
	}
}
