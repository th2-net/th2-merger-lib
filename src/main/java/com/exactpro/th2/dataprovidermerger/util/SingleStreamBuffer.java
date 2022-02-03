package com.exactpro.th2.dataprovidermerger.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.dataprovider.grpc.StreamResponse;

import io.grpc.stub.StreamObserver;

public class SingleStreamBuffer implements StreamObserver<StreamResponse> {
	
	private static final Logger logger = LoggerFactory.getLogger(SingleStreamBuffer.class);
	
	private BlockingQueue<StreamResponse> queue = new LinkedBlockingQueue<>();

	private volatile boolean streamCompleted = false;
	
	private volatile boolean completedWithError = false;
	
	public SingleStreamBuffer(Integer messageQueueSizeLimit) {
		
		if(messageQueueSizeLimit != null) {
			queue = new LinkedBlockingQueue<>(messageQueueSizeLimit);
		} else {
			queue = new LinkedBlockingQueue<>();
		}
		
	}
	
	@Override
	public void onNext(StreamResponse value) {
		
		try {
			queue.put(value);
		} catch (InterruptedException e) {
			logger.error("Interrupted", e);
			completedWithError = true;
			streamCompleted = true;
		}
		
	}

	@Override
	public void onError(Throwable t) {
		completedWithError = true;
		streamCompleted = true;
		logger.error("Error on stream processing", t);
		
	}

	@Override
	public void onCompleted() {
		streamCompleted = true;
	}
	
	public BlockingQueue<StreamResponse> getQueue() {
		return queue;
	}

	public boolean isStreamCompleted() {
		return streamCompleted;
	}

	public boolean isCompletedWithError() {
		return completedWithError;
	}
	
}
