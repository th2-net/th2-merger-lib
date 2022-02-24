package com.exactpro.th2.mergerlib.test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.dataprovider.grpc.MessageSearchResponse;
import com.exactpro.th2.dataprovider.grpc.MessageStream;
import com.exactpro.th2.dataprovider.grpc.MessageStreamsRequest;
import com.exactpro.th2.dataprovider.grpc.MessageStreamsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.dataprovider.grpc.DataProviderGrpc;
import com.exactpro.th2.dataprovider.grpc.DataProviderGrpc.DataProviderBlockingStub;
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest;
import com.exactpro.th2.dataprovider.grpc.TimeRelation;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Timestamp;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class LdpConnectivityTest {

	private static final Logger logger = LoggerFactory.getLogger(LdpConnectivityTest.class);

	private static Iterable<MessageStream> getStreams(List<String> streams) {
		List<MessageStream> out = new ArrayList<>(streams.size() * 2);
		for (String stream : streams) {
			out.add(MessageStream.newBuilder().setName(stream).setDirection(Direction.FIRST).build());
			out.add(MessageStream.newBuilder().setName(stream).setDirection(Direction.SECOND).build());
		}
		return out;
	}

	public static void main(String[] args) throws InterruptedException {

		String target = "localhost:32572";

		ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
		try {

			List<String> streamId = Arrays.asList(new String[] { "LoadTestMessages" });

			Instant from = LocalDateTime.of(2022, 2, 1, 8, 16, 0).atZone(ZoneId.of("UTC")).toInstant(); // Instant.now();
			Instant to = from.plus(10, ChronoUnit.HOURS);

			logger.info("From - to: {} - {}", from, to);

			MessageSearchRequest.Builder messageSearchBuilder = MessageSearchRequest.newBuilder()
					.setStartTimestamp(timestampFromInstant(from)).setEndTimestamp(timestampFromInstant(to))
					.setSearchDirection(TimeRelation.NEXT)
					.addAllStream(getStreams(streamId))
					.setResultCountLimit(Int32Value.of(100));

			DataProviderBlockingStub client = DataProviderGrpc.newBlockingStub(channel);

			// DataProviderStub client2 = DataProviderGrpc.newStub(channel);

			MessageStreamsResponse streams = client.getMessageStreams(MessageStreamsRequest.getDefaultInstance());

			Iterator<MessageSearchResponse> it = client.searchMessages(messageSearchBuilder.build());

			logger.info("Streams: {}", streams);

			while (it.hasNext()) {
				MessageSearchResponse r = it.next();
				logger.info("Response: {}", r);
			}

			logger.info("Stream read finished");

		} finally {
			// ManagedChannels use resources like threads and TCP connections. To prevent
			// leaking these
			// resources the channel should be shut down when it will no longer be used. If
			// it may be used
			// again leave it running.
			channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
		}

	}

	private static Timestamp timestampFromInstant(Instant instant) {
		return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build();
	}

}
