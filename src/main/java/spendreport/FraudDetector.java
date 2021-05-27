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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

/**
 * Skeleton code for implementing a fraud detector.
 */
public class FraudDetector extends KeyedProcessFunction<Long, Float[], Alert> {

	private static final long serialVersionUID = 1L;
	//private transient ValueState<Boolean> flagState;

	private float[][] recentVectors = new float[30][6];



	@Override
	public void processElement(
			Float[] dataVector,
			Context context,
			Collector<Alert> collector) throws Exception {

		System.out.println("dataVector: " + dataVector.toString());


	}

	@Override
	public void open(Configuration parameters) {	//rozpoczęcie procesu przetwarzania

		/*ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
				"flag",
				Types.BOOLEAN
		);*/

		//flagState = getRuntimeContext().getState(flagDescriptor);
	}
}
