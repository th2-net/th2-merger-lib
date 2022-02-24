/*******************************************************************************
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.exactpro.th2.dataprovidermerger.util;

import java.time.Instant;

import com.exactpro.th2.dataprovider.grpc.MessageSearchResponse;
import com.google.protobuf.Timestamp;

public class MergerUtil {
	
	public static Instant extractTimestampFromMessage(MessageSearchResponse response, Timestamp defaultValue) {
		
		Timestamp ts = defaultValue;
		
		if(response.hasMessage()
				&& response.getMessage().hasTimestamp()) {
			ts = response.getMessage().getTimestamp();
		}
		
		return instantFromTimestamp(ts);
		
	}
	
	public static Instant instantFromTimestamp(Timestamp ts) {
		return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
	}
	
}
