package com.exactpro.th2.dataprovidermerger.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.exactpro.th2.dataprovider.grpc.DataProviderGrpc;
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.dataprovider.grpc.StreamResponse;

import io.grpc.stub.StreamObserver;

public class SingleStreamBuffer implements StreamObserver<StreamResponse> {
	
	private static final Logger logger = LoggerFactory.getLogger(SingleStreamBuffer.class);
	
	private BlockingQueue<StreamResponse> queue = new LinkedBlockingQueue<>();

	private MessageSearchRequest request;

	private DataProviderGrpc.DataProviderStub client;

	private StreamResponse prev = null;

	private volatile boolean streamCompleted = false;
	
	private volatile boolean completedWithError = false;

	private int maxSize = 0;

	public SingleStreamBuffer() {
	}
	
	@Override
	public void onNext(StreamResponse value) {
		
		try {
			queue.put(value);
			maxSize++;
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

	public void reboot() {
		streamCompleted = false;
		maxSize = 0;
	}

	public int getMaxSize() {
		return maxSize;
	}

	public MessageSearchRequest getMessageSearchRequest() {
		return request;
	}

	public void setMessageSearchRequest(MessageSearchRequest request) {
		this.request = request;
	}

	public DataProviderGrpc.DataProviderStub getDataProviderStub() {
		return client;
	}

	public void setDataProviderStub(DataProviderGrpc.DataProviderStub client) {
		this.client = client;
	}

	public StreamResponse getPrevStreamResponse() {
		return prev;
	}

	public void setPrevStreamResponse(StreamResponse prev) {
		this.prev = prev;
	}
}
