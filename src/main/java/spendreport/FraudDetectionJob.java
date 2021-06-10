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
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.FileWriter;


public class FraudDetectionJob {
	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//		DataStream<Tuple7<Integer, Double, Double, Double, Double, Double, Double>> dataStream = env.readTextFile("/Users/bartoszcybulski/Documents/workspaces/java_workspace/psd_projekt/PSDProject/src/main/resources" +
//						"/mock_data_short.csv")	//TODO zmienic sciezke dla obecnej maszyny
		DataStream<Tuple7<Integer, Double, Double, Double, Double, Double, Double>> dataStream = env.readTextFile(
				"/home/george/Pulpit/Projekt PSD/PSDProject/src/main/resources/result_50K.csv")
				.flatMap(new DataSplitter())
				.keyBy(value -> value.f0)
				.countWindow(30, 1)
				.process(new FraudDetector());

		env.execute("Data analyse");
	}

	public static class DataSplitter implements FlatMapFunction<String, Tuple2<Integer, Double>> {

		@Override
		public void flatMap(String text, Collector<Tuple2<Integer, Double>> output) throws Exception {

			Double itemInRow = 0.0;
			for (String line: text.split("\n")) {

				String[] elements = line.split(",");
				for (int i = 0; i < elements.length; i++) {
					itemInRow = Double.valueOf(elements[i]);
					output.collect(Tuple2.of(Integer.valueOf(i), itemInRow));
				}
			}
		}
	}
}
