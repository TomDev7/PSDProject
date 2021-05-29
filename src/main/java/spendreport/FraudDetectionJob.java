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
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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

		DataStream<Tuple6<Float, Float, Float, Float, Float, Float>> dataStream = env.readTextFile("src/main/resources/mock_data.csv")
				.flatMap(new Splitter());
				//.keyBy(value -> value.f0);	//TODO add some key to data, like datetime of entry. Use it here instead of value[0]


		//System.out.println("dataStream: ");
		//dataStream.print();

		DataStream<Alert> alerts = dataStream
				.process(new FraudDetector())	//na każdej z tych grup uruchomienie przetwarzania FraudDetectorem (bo to równoległe przetwarzanie na każdej z grup)
				.name("data-analyser");

		alerts
				.addSink(new AlertSink())	// czyli sink może być 'customowy'? np wysyłanie gdzieś po REST API
				.name("send-alerts");


		env.execute("Data analyse");
	}

	public static class Splitter implements FlatMapFunction<String, Tuple6<Float, Float, Float, Float, Float, Float>> {

		@Override
		public void flatMap(String text, Collector<Tuple6<Float, Float, Float, Float, Float, Float>> output) throws Exception {

			for (String line: text.split("\n")) {

				Float[] dataVector = new Float[6];

				String[] elements = line.split(";");
				for (int i = 0; i < elements.length; i++) {
					dataVector[i] = Float.parseFloat(elements[i]);
				}

				output.collect(Tuple6.of(dataVector[0], dataVector[1], dataVector[2], dataVector[3], dataVector[4], dataVector[5]));
			}
		}
	}
}
