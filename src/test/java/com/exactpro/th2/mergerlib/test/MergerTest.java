package com.exactpro.th2.mergerlib.test;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest;
import com.exactpro.th2.dataprovider.grpc.StreamResponse;
import com.exactpro.th2.dataprovider.grpc.StringList;
import com.exactpro.th2.dataprovider.grpc.TimeRelation;
import com.exactpro.th2.dataprovidermerger.Th2DataProviderMessagesMerger;
import com.exactpro.th2.dataprovidermerger.util.MergerUtil;
import com.exactpro.th2.dataprovidermerger.util.TimestampComparator;
import com.exactpro.th2.mergerlib.test.testsuit.InMemoryGrpcServer;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Timestamp;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class MergerTest {
	
	private static final Logger logger = LoggerFactory.getLogger(MergerTest.class);
	
	private final int port = 8087;
	
	private List<MessageSearchRequest> createRequests(int num) {
		
		List<MessageSearchRequest> requests = new ArrayList<>();
		
		for(int i = 0; i < num; i++) {
		
			List<String> streamId = Arrays.asList(new String[] {"LoadTestMessages-" + i});
	    	
	    	Instant from = LocalDateTime.of(2022, 2, 1, 8, 16, 0)
	    			.atZone(ZoneId.of("UTC")).toInstant(); //  Instant.now();
	    	Instant to = from.plus(10, ChronoUnit.HOURS);
	    	
	    	logger.info("From - to: {} - {}", from, to);
	    	
	    	MessageSearchRequest.Builder messageSearchBuilder = MessageSearchRequest.newBuilder()
	                .setStartTimestamp(timestampFromInstant(from))
	                .setEndTimestamp(timestampFromInstant(to))
	                .setSearchDirection(TimeRelation.NEXT)
	                .setStream(StringList.newBuilder().addAllListString(streamId).build());
	    	
	    	requests.add( messageSearchBuilder.build() );
	    	
		}
    	
    	return requests;
		
	}
	
	private Timestamp timestampFromInstant(Instant instant) {
		return Timestamp.newBuilder().setSeconds(instant.getEpochSecond())
			    .setNanos(instant.getNano()).build();
	}
	
	@Test
	public void testFakeServer3Streams() throws IOException, InterruptedException {
		
		logger.info("Test 3 streams");
		
		InMemoryGrpcServer srv = new InMemoryGrpcServer(port);
		srv.start();
		
		String target = "localhost:" + port;
		
		ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
	        .usePlaintext()
	        .build();
		
		Th2DataProviderMessagesMerger merger = new Th2DataProviderMessagesMerger(channel);
		
		Iterator<StreamResponse> it = merger.searchMessages(createRequests(3),
				new TimestampComparator().reversed());
		
		Timestamp defaultTs = Timestamp.newBuilder().setSeconds(0).build();
		Instant lastTimestamp = MergerUtil.instantFromTimestamp(defaultTs);
		
		while (it.hasNext()) {
            StreamResponse r = it.next();
            
            Instant currentTimestamp = MergerUtil.extractTimestampFromMessage(r, defaultTs);
            if(lastTimestamp != null) {
            	// assert that we receive messages in correct order
            	assertTrue(lastTimestamp.compareTo(currentTimestamp) < 0);
            }
            lastTimestamp = currentTimestamp;
            
            logger.info("Response: {}", r);
   	 }
   	
   	 logger.info("Stream read finished");
		
	}
	
}
