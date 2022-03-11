package com.exactpro.th2.mergerlib.test;

import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.MessageMetadata;
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest;
import com.exactpro.th2.dataprovider.grpc.MessageSearchResponse;
import com.exactpro.th2.dataprovider.grpc.MessageStream;
import com.exactpro.th2.dataprovider.grpc.TimeRelation;
import com.exactpro.th2.dataprovidermerger.Th2DataProviderMessagesMerger;
import com.exactpro.th2.dataprovidermerger.util.MergerUtil;
import com.exactpro.th2.dataprovidermerger.util.TimestampComparator;
import com.exactpro.th2.mergerlib.test.testsuit.InMemoryGrpcServer;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PaginationTest {
	
	private static final Logger logger = LoggerFactory.getLogger(PaginationTest.class);
	
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
					.addStream(MessageStream.newBuilder().setName(streamId.get(0)).setDirection(Direction.FIRST).build());

	    	requests.add( messageSearchBuilder.build() );
	    	
		}
    	
    	return requests;
		
	}
	
	private Timestamp timestampFromInstant(Instant instant) {
		return Timestamp.newBuilder().setSeconds(instant.getEpochSecond())
			    .setNanos(instant.getNano()).build();
	}

	MergerTest.TestBadMessage good = (StreamObserver<MessageSearchResponse> responseObserver, MessageSearchRequest request) -> {
		MessageMetadata metadata = MessageMetadata.newBuilder()
				.setTimestamp(timestampFromInstant(Instant.now()))
				.build();
		return Message.newBuilder()
				.setMetadata(metadata)
				.build();
	};

	@Test
	public void testFakeServer3StreamsWithThreeMerges() throws Exception {

		logger.info("Test 3 streams with three merges");

		String srvName = InProcessServerBuilder.generateName();
		ExecutorService executorService = Executors.newFixedThreadPool(5);

		InMemoryGrpcServer srv = new InMemoryGrpcServer(srvName, executorService, good);

		srv.start();

		int limit = 0;
		Th2DataProviderMessagesMerger merger = new Th2DataProviderMessagesMerger(new TestCommonFactory(srvName, executorService));

		int countOfRequests1 = 3;
		Iterator<MessageSearchResponse> it = merger.searchMessages(createRequests(countOfRequests1),
				new TimestampComparator().reversed());

		int countOfRequests2 = 4;
		Iterator<MessageSearchResponse> it2 = merger.searchMessages(createRequests(countOfRequests2),
				new TimestampComparator().reversed());

		int countOfRequests3 = 2;
		Iterator<MessageSearchResponse> it3 = merger.searchMessages(createRequests(countOfRequests3),
				new TimestampComparator().reversed());

		Timestamp defaultTs = Timestamp.newBuilder().setSeconds(0).build();
		Instant lastTimestamp = MergerUtil.instantFromTimestamp(defaultTs);

		int messages = srv.getNumMsgs();
		int lists = (int)Math.ceil((double)messages / limit);

		getResponse(it, defaultTs, lastTimestamp, countOfRequests1, messages);
		getResponse(it2, defaultTs, lastTimestamp, countOfRequests2, messages);
		getResponse(it3, defaultTs, lastTimestamp, countOfRequests3, messages);

		// assert that we get a number of sheets equal to the expected number of sheets
		assertEquals(srv.getCountOfLists(), lists*countOfRequests1 + lists*countOfRequests2 + lists*countOfRequests3);
	}

	private void getResponse(Iterator<MessageSearchResponse> it, Timestamp defaultTs, Instant lastTimestamp, int requests, int messages) {
		int count = 0;
		while (it.hasNext()) {
			MessageSearchResponse r = it.next();

			Instant currentTimestamp = MergerUtil.extractTimestampFromMessage(r, defaultTs);
			if(lastTimestamp != null) {
				// assert that we receive messages in correct order
				assertTrue(lastTimestamp.compareTo(currentTimestamp) <= 0);
			}
			lastTimestamp = currentTimestamp;

			logger.info("Response: {}", r);
			count++;
		}

		logger.info("Stream read finished " + count);
		// assert that we receive a number of messages equal to the expected number of messages
		assertEquals((messages * requests), count);
	}
	
}
