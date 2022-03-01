package com.exactpro.th2.mergerlib.test.testsuit;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.mergerlib.test.MergerTest;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Int32Value;
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
	
	private int port;
	private io.grpc.Server server;
	private int numMsgs = 36;
	private int countOfLists = 0;
	private MergerTest.TestBadMessage badMessage;
	
	private ScheduledExecutorService executorService =
			Executors.newSingleThreadScheduledExecutor();

	public InMemoryGrpcServer(int port) {
		this.port = port;
	}

	public InMemoryGrpcServer(int port, MergerTest.TestBadMessage badMessage) {
		this.port = port;
		this.badMessage = badMessage;
	}
	
	public void start() throws IOException, InterruptedException {
        server = ServerBuilder.forPort(port)
          .addService(new DataProvider(numMsgs))
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

		private final int numMsgs;

		public DataProvider(int numMsgs){
			this.numMsgs = numMsgs;
		}
		@Override
		public void searchMessages(MessageSearchRequest request,
		        StreamObserver<StreamResponse> responseObserver) {
			
			logger.info("Search messages request");
			
			countOfLists++;
			long prevInnerMessageId = request.getResumeFromId().getSequence();

			long remainder = numMsgs - prevInnerMessageId;

			if(request.getResultCountLimit() != Int32Value.getDefaultInstance()){
				int limit = request.getResultCountLimit().getValue();
				remainder = (double)remainder / limit > 1 ? limit : remainder;
			}

			for (int i = 1; i <= remainder; i++) {
				MessageID messageID = MessageID.newBuilder().setSequence(prevInnerMessageId+i).build();
				executorService.schedule(() -> {
					Message msg = badMessage.createBadMessage(responseObserver, messageID, request);
					MessageData md = MessageData.newBuilder()
							.setMessage(msg).setMessageId(messageID)
							.build();
					
					StreamResponse resp = StreamResponse.newBuilder()
						.setMessage(md)
			          .build();
					
			        responseObserver.onNext(resp);
				},
					1,
					TimeUnit.MILLISECONDS);

		    }
			
			executorService.schedule(() -> {
				responseObserver.onCompleted();
				logger.info("Search messages response completed");
			},
					1,
				TimeUnit.MILLISECONDS);
			
		}
		
		private Timestamp generateTimestamp() {
			return timestampFromInstant(Instant.now());
		}
		
		private Timestamp timestampFromInstant(Instant instant) {
			return Timestamp.newBuilder().setSeconds(instant.getEpochSecond())
				    .setNanos(instant.getNano()).build();
		}
		
	}

	public int getNumMsgs(){
		return numMsgs;
	}

	public int getCountOfLists(){
		return countOfLists;
	}
	
}
