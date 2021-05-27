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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Skeleton code for the datastream walkthrough
 */
public class InvestmentDataAnalyseJob {
	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Float[]> dataStream = env.readTextFile("src/main/resources/mock_data.csv")
				.flatMap(new Splitter())
				.keyBy(value -> value[0])	//TODO add some key to data, like datetime of entry. Use it here instead of value[0]
				.sum(1);	//TODO ?

		dataStream.print();
		env.execute("Fraud Detection");



		/*DataStream<Tuple1<Float>> transactions = env
			.addSource(mockData)
			.name("transactions");

		DataStream<Alert> alerts = mockData
			.keyBy(Transaction::getAccountId)	//pogrupowanie transakcji w grupy o tym samym account ID
			.process(new FraudDetector())	//na każdej z tych grup uruchomienie przetwarzania FraudDetectorem (bo to równoległe przetwarzanie na każdej z grup)
			.name("fraud-detector");

		alerts
			.addSink(new AlertSink())	// czyli sink może być 'customowy'? np wysyłanie gdzieś po REST API
			.name("send-alerts");*/
	}

	public static class Splitter implements FlatMapFunction<String, Float[]> {

		@Override
		public void flatMap(String text, Collector<Float[]> output) throws Exception {

			for (String line: text.split("\n")) {

				Float[] dataVector = new Float[6];

				String[] elements = line.split(";");
				for (int i = 0; i < elements.length; i++) {
					dataVector[i] = Float.parseFloat(elements[i]);
				}

				output.collect(dataVector);
			}
		}
	}
}
