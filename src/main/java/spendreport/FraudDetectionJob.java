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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

/**
 * Skeleton code for the datastream walkthrough
 */
public class FraudDetectionJob {
	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


//		DataStream<Tuple7<Integer, Double, Double, Double, Double, Double, Double>> dataStream = env.readTextFile("/home/george/Pulpit/Projekt PSD/PSDProject/src/main/resources/mock_data.csv")	//TODO zmienic sciezke dla obecnej maszyny
		DataStream<Tuple7<Integer, Double, Double, Double, Double, Double, Double>> dataStream = env.readTextFile(
				"/Users/bartoszcybulski/Documents/workspaces/java_workspace/psd_projekt/PSDProject/src/main/resources" +
						"/mock_data_short.csv")	//TODO zmienic sciezke dla obecnej maszyny
				.flatMap(new Splitter())
				.keyBy(value -> value.f0)
				.countWindow(30, 1)
				.process(new FraudDetector());

		//System.out.println("Results: " + dataStream.print());

		//System.out.println("dataStream: ");
		//System.out.println(dataStream.toString());
		//dataStream.print();

//		DataStream<Alert> alerts = dataStream
//				.process(new FraudDetector())	//na każdej z tych grup uruchomienie przetwarzania FraudDetectorem (bo to równoległe przetwarzanie na każdej z grup)
//				.name("data-analyser");
//
//		alerts.addSink(new AlertSink())	// czyli sink może być 'customowy'? np wysyłanie gdzieś po REST API
//				.name("data-analyse-alerts");


		env.execute("Data analyse");
	}

//	public WindowedStream<T, KEY, GlobalWindow> countWindow(long size, long slide) {
//		return window(GlobalWindows.create())
//				.evictor(CountEvictor.of(size))
//				.trigger(CountTrigger.of(slide));
//	}

	public static class Splitter implements FlatMapFunction<String, Tuple2<Integer, Double>> {

		@Override
		public void flatMap(String text, Collector<Tuple2<Integer, Double>> output) throws Exception {

			Double sumOfAllItemsInRow = 0.0;
			Double itemInRow = 0.0;
			for (String line: text.split("\n")) {

				String[] elements = line.split(",");
				for (int i = 0; i < elements.length; i++) {
					itemInRow = Double.valueOf(elements[i]);
					// TODO modify sum to some proper calculation
					sumOfAllItemsInRow += itemInRow;
					output.collect(Tuple2.of(Integer.valueOf(i), itemInRow));
				}

				output.collect(Tuple2.of(Integer.valueOf(999), itemInRow));

			}
		}
	}
}
