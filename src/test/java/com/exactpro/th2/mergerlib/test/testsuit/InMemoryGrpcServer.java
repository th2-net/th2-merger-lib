package com.exactpro.th2.mergerlib.test.testsuit;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.grpc.inprocess.InProcessServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageMetadata;
import com.exactpro.th2.dataprovider.grpc.DataProviderGrpc.DataProviderImplBase;
import com.exactpro.th2.dataprovider.grpc.MessageData;
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest;
import com.exactpro.th2.dataprovider.grpc.StreamResponse;
import com.google.protobuf.Timestamp;

import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

public class InMemoryGrpcServer {
	
	private static final Logger logger = LoggerFactory.getLogger(InMemoryGrpcServer.class);

	private io.grpc.Server server;

	private final String name;
	private final ExecutorService executorService;

	private ScheduledExecutorService scheduler =
			Executors.newSingleThreadScheduledExecutor();
	
	public InMemoryGrpcServer(String name, ExecutorService executorService) {
		this.name = name;
		this.executorService = executorService;
	}
	
	public void start() throws IOException, InterruptedException {
		server = InProcessServerBuilder
				.forName(name)
				.executor(executorService)
				.addService(new DataProvider())
				.build();
        
        server.start();
        
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
              // Use stderr here since the logger may have been reset by its JVM shutdown hook.
              System.err.println("*** shutting down gRPC server since JVM is shutting down");
              try {
            	  server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                e.printStackTrace(System.err);
              }
              System.err.println("*** server shut down");
            }
          });
        
        //server.awaitTermination();
        
	}
	
	private class DataProvider extends DataProviderImplBase {

		Instant instant = Instant.now();

		private synchronized Instant getTime() {
			long l = System.currentTimeMillis();
			Instant old = instant;
			instant = instant.plusSeconds(60);
			return old;
		}

		@Override
		public void searchMessages(MessageSearchRequest request,
		        StreamObserver<StreamResponse> responseObserver) {
			
			logger.info("Search messages request");
			
			int numMsgs = 5;
			
			for (int i = 1; i <= numMsgs; i++) {

				Instant time = getTime();
				MessageMetadata metadata = MessageMetadata.newBuilder()
							.setTimestamp(Timestamp.newBuilder().setSeconds(time.getEpochSecond()).setNanos(time.getNano()))
							.build();

					Message msg = Message.newBuilder()
							.setMetadata(metadata)
							.build();

					MessageData md = MessageData.newBuilder()
							.setMessage(msg)
							.build();

					StreamResponse resp = StreamResponse.newBuilder()
						.setMessage(md)
			          .build();

			        responseObserver.onNext(resp);

					logger.debug("Send message with time: " + getTime());

		    }

			responseObserver.onCompleted();
			logger.info("Search messages response completed");
			
		}
		
		private Timestamp generateTimestamp() {
			return timestampFromInstant(Instant.now());
		}
		
		private Timestamp timestampFromInstant(Instant instant) {
			return Timestamp.newBuilder().setSeconds(instant.getEpochSecond())
				    .setNanos(instant.getNano()).build();
		}
		
	}
	
}
