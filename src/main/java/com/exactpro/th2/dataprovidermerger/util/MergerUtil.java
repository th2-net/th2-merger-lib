package com.exactpro.th2.dataprovidermerger.util;

import java.time.Instant;

import com.exactpro.th2.dataprovider.grpc.StreamResponse;
import com.google.protobuf.Timestamp;

public class MergerUtil {
	
	public static Instant extractTimestampFromMessage(StreamResponse response, Timestamp defaultValue) {
		
		Timestamp ts = defaultValue;
		
		if(response.hasMessage()
				&& response.getMessage().hasMessage()
				&& response.getMessage().getMessage().hasMetadata()
				&& response.getMessage().getMessage().getMetadata().hasTimestamp()) {
			ts = response.getMessage().getMessage().getMetadata().getTimestamp();
		}
		
		return instantFromTimestamp(ts);
		
	}
	
	public static Instant instantFromTimestamp(Timestamp ts) {
		return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
	}
	
}
