package com.exactpro.th2.dataprovidermerger.util;

import java.time.Instant;
import java.util.Comparator;

import com.exactpro.th2.dataprovider.grpc.StreamResponse;
import com.google.protobuf.Timestamp;

public class TimestampComparator implements Comparator<StreamResponse> {
	
	private final Timestamp defaultTimestamp = 
			Timestamp.newBuilder().setSeconds(0).build();
	
	@Override
	public int compare(StreamResponse first, StreamResponse second) {
		
		Instant firstTs = MergerUtil.extractTimestampFromMessage(first, defaultTimestamp);
		Instant secondTs = MergerUtil.extractTimestampFromMessage(second, defaultTimestamp);
		
		return firstTs.compareTo(secondTs);
		
	}	
	
}
