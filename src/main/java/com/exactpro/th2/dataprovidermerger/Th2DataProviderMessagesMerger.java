package com.exactpro.th2.dataprovidermerger;

import java.util.*;

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

	public Th2DataProviderMessagesMerger(ManagedChannel channel) {
		this.channel = channel;
	}

	public Iterator<StreamResponse> searchMessages(List<MessageSearchRequest> searchOptions,
												   Comparator<StreamResponse> responseComparator) {

		MergeIterator mergeIterator = new MergeIterator(new ArrayList<>(), responseComparator);

		for(MessageSearchRequest request : searchOptions) {

			SingleStreamBuffer buffer = new SingleStreamBuffer();

			DataProviderStub client = DataProviderGrpc.newStub(channel);

			client.searchMessages(request, buffer);

			buffer.setMessageSearchRequest(request);

			buffer.setDataProviderStub(client);

			mergeIterator.getBuffers().add(buffer);

		}

		return mergeIterator;
	}

	private class MergeIterator implements Iterator<StreamResponse> {

		private StreamResponse next = null;

		private final List<SingleStreamBuffer> buffers;

		private final Comparator<StreamResponse> responseComparator;

		public MergeIterator(List<SingleStreamBuffer> buffers, Comparator<StreamResponse> responseComparator){
			this.buffers = buffers;
			this.responseComparator = responseComparator;
		}

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

		private void debugQueues() {
			StringBuilder sb = new StringBuilder();

			for (SingleStreamBuffer buffer : buffers) {
				sb.append("-")
						.append(buffer.getQueue().size());
			}

			logger.debug("Queue sizes: {}", sb.toString());
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

								MessageSearchRequest request = buffer.getMessageSearchRequest();

								if(buffer.getMaxSize() < request.getResultCountLimit().getValue()){
									continue;
								}

								MessageSearchRequest.Builder messageSearchBuilder = MessageSearchRequest.newBuilder()
										.setStartTimestamp(request.getStartTimestamp())
										.setEndTimestamp(request.getEndTimestamp())
										.setSearchDirection(request.getSearchDirection())
										.setStream(request.getStream())
										.setResultCountLimit(request.getResultCountLimit());
								if(buffer.getPrevStreamResponse() != null){
									messageSearchBuilder.setResumeFromId(buffer.getPrevStreamResponse().getMessage().getMessageId());
								}

								request = messageSearchBuilder.build();

								buffer.reboot();

								buffer.setMessageSearchRequest(request);

								buffer.getDataProviderStub().searchMessages(request, buffer);

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
							if(next == null){
								nextBuffer.setPrevStreamResponse(nextBuffer.getQueue().peek());
							}
							else{
								nextBuffer.setPrevStreamResponse(next);
							}
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

		public List<SingleStreamBuffer> getBuffers(){
			return buffers;
		}

		public Comparator<StreamResponse> getResponseComparator(){
			return responseComparator;
		}

	}

}
