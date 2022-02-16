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

		MergeIterator mergeIterator = new MergeIterator(new HashMap<>(), responseComparator, new HashMap<>());

		for(MessageSearchRequest request : searchOptions) {

			SingleStreamBuffer buffer = new SingleStreamBuffer();

			DataProviderStub client = DataProviderGrpc.newStub(channel);

			client.searchMessages(request, buffer);

			mergeIterator.getClients().put(buffer, client);

			mergeIterator.getBuffers().put(buffer, request);

		}

		return mergeIterator;
	}

	private class MergeIterator implements Iterator<StreamResponse> {

		private StreamResponse next = null;
		private StreamResponse prev = null;

		private final HashMap<SingleStreamBuffer, MessageSearchRequest> buffers;

		private final HashMap<SingleStreamBuffer, DataProviderStub> clients;

		private final Comparator<StreamResponse> responseComparator;

		public MergeIterator(HashMap<SingleStreamBuffer, MessageSearchRequest> buffers, Comparator<StreamResponse> responseComparator, HashMap<SingleStreamBuffer, DataProviderStub> clients){
			this.buffers = buffers;
			this.responseComparator = responseComparator;
			this.clients = clients;
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
				prev = next;
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

			for (SingleStreamBuffer buffer : buffers.keySet()) {
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

					for(SingleStreamBuffer buffer : buffers.keySet()) {

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

								MessageSearchRequest request = buffers.get(buffer);

								if(buffer.getMaxSize() < request.getResultCountLimit().getValue()){
									continue;
								}

								MessageSearchRequest.Builder messageSearchBuilder = MessageSearchRequest.newBuilder()
										.setStartTimestamp(request.getStartTimestamp())
										.setEndTimestamp(request.getEndTimestamp())
										.setSearchDirection(request.getSearchDirection())
										.setStream(request.getStream())
										.setResultCountLimit(request.getResultCountLimit());
								if(prev != null){
									messageSearchBuilder.setResumeFromId(prev.getMessage().getMessageId());
								}

								request = messageSearchBuilder.build();

								buffer.reboot();

								buffers.put(buffer, request);

								clients.get(buffer).searchMessages(request, buffer);

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

		public HashMap<SingleStreamBuffer, MessageSearchRequest>  getBuffers(){
			return buffers;
		}

		public HashMap<SingleStreamBuffer, DataProviderStub> getClients(){
			return clients;
		}

		public Comparator<StreamResponse> getResponseComparator(){
			return responseComparator;
		}

		public StreamResponse getPrev(){
			return prev;
		}

	}

}
