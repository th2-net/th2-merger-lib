package com.exactpro.th2.dataprovidermerger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.dataprovider.grpc.DataProviderGrpc;
import com.exactpro.th2.dataprovider.grpc.DataProviderGrpc.DataProviderStub;
import com.exactpro.th2.dataprovidermerger.util.SingleStreamBuffer;
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest;
import com.exactpro.th2.dataprovider.grpc.StreamResponse;

import io.grpc.ManagedChannel;

public class Th2DataProviderMessagesMerger {
	
	private static final long SLEEP_TIME = 50;
	
	private static final Logger logger = LoggerFactory.getLogger(Th2DataProviderMessagesMerger.class);
	
	private ManagedChannel channel;
	
	private List<SingleStreamBuffer> buffers;
	
	private Comparator<StreamResponse> responseComparator;
	
	private Integer messageQueueSizeLimit = null;
	
	public Th2DataProviderMessagesMerger(ManagedChannel channel) {
		this.channel = channel;
	}
	
	public Th2DataProviderMessagesMerger(ManagedChannel channel, Integer messageQueueSizeLimit) {
		this.channel = channel;
		this.messageQueueSizeLimit = messageQueueSizeLimit;
	}
	
	private void debugQueues() {
		StringBuilder sb = new StringBuilder();
		
		for (SingleStreamBuffer buffer : buffers) {
			sb.append("-")
			.append(buffer.getQueue().size());
		}
		
		logger.debug("Queue sizes: {}", sb.toString());
	}
	
	public Iterator<StreamResponse> searchMessages(List<MessageSearchRequest> searchOptions,
			Comparator<StreamResponse> responseComparator) {
		
		this.buffers = new ArrayList<>();
		this.responseComparator = responseComparator;
		
		for(MessageSearchRequest request : searchOptions) {
			
			SingleStreamBuffer buffer = new SingleStreamBuffer(messageQueueSizeLimit);
			
			DataProviderStub client = DataProviderGrpc.newStub(channel);
			
			client.searchMessages(request, buffer);
			
			this.buffers.add(buffer);
			
		}
		
		return new MergeIterator();
	}
	
	private StreamResponse getNextMessageBlocking() {
		
		try {
			
			boolean notCompletedBufferExists = false;
			
			do {
			
				SingleStreamBuffer nextBuffer = null;
				StreamResponse nextResponse = null;
				boolean canProvideNext = true;
				
				for(SingleStreamBuffer buffer : buffers) {
					
					if(buffer.isCompletedWithError()) {
						throw new IllegalStateException("One of the message streams was closed with an error");
					}
					
					boolean bufferStreamCompleted = buffer.isStreamCompleted();
					notCompletedBufferExists |= !bufferStreamCompleted;
					StreamResponse bufferMessage = buffer.getQueue().peek();
					
					if(bufferMessage != null) {
						
						if(nextResponse == null
								|| responseComparator.compare(bufferMessage, nextResponse) > 0) {
							nextResponse = bufferMessage;
							nextBuffer = buffer;
						}
						
					} else {
						if(bufferStreamCompleted) {
							continue;
						} else {
							canProvideNext = false;
							break;
						}
					}
					
				}
				
				if(nextResponse != null) {
					if(logger.isDebugEnabled()) {
						debugQueues();
					}
					
					if(canProvideNext) {
						return nextBuffer.getQueue().poll();
					} else {
						Thread.sleep(SLEEP_TIME);
					}
				}
			
			} while(notCompletedBufferExists);
			
			return null; // no more messages left
		
		} catch(Exception e) {
			throw new IllegalStateException("Unable to retrieve next message", e);
		}
		
	}
	
	private class MergeIterator implements Iterator<StreamResponse> {
		
		private StreamResponse next = null;
		
		@Override
		public boolean hasNext() {
			
			if(next != null) {
				return true;
			}
			
			next = getNextMessageBlocking();
			
			return next != null;
		}

		@Override
		public StreamResponse next() {
			
			StreamResponse result = null;
			
			if(next != null) {
				result = next;
				next = null;
			} else {
				result = getNextMessageBlocking(); 
			}
			
			if(result == null) {
				throw new IllegalStateException("No elements available");
			}
			
			return result;
		}

	}
	
}
