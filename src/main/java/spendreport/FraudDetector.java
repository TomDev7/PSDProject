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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;


public class FraudDetector extends ProcessWindowFunction<Tuple2<Integer, Float>, Tuple2<Integer, Float>, Integer, GlobalWindow> {

	private static final long serialVersionUID = 1L;


	/*@Override
	public void processElement(
			Tuple2<Integer, Float> dataTuple,
			Context context,
			Collector<Alert> collector) throws Exception {

		System.out.println("dataVector: " + dataTuple.toString());

		Alert alert = new Alert();
		alert.setId(Long.parseLong(dataTuple.f0.toString()));
		collector.collect(alert);


	}*/

	@Override
	public void open(Configuration parameters) {	//rozpoczęcie procesu przetwarzania

		/*ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
				"flag",
				Types.BOOLEAN
		);*/

		//flagState = getRuntimeContext().getState(flagDescriptor);
	}

	@Override
	public void process(Integer key, Context context, Iterable<Tuple2<Integer, Float>> input, Collector<Tuple2<Integer, Float>> out) throws Exception {

		System.out.println("process window: " + context.window());
		float count = 0;
		for (Tuple2<Integer, Float> in: input) {
			count += in.f1;
		}
//		out.collect("Window: " + context.window() + "count: " + count);
		out.collect(Tuple2.of(key, Float.valueOf(count)));
	}

}
