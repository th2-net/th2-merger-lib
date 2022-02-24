/*******************************************************************************
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.exactpro.th2.dataprovidermerger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import com.exactpro.th2.common.schema.factory.CommonFactory;
import com.exactpro.th2.dataprovider.grpc.DataProviderService;
import com.exactpro.th2.dataprovider.grpc.MessageSearchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.dataprovidermerger.util.SingleStreamBuffer;
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest;


public class Th2DataProviderMessagesMerger {
	
	private static final Logger logger = LoggerFactory.getLogger(Th2DataProviderMessagesMerger.class);
	
	private CommonFactory commonFactory;
	private List<SingleStreamBuffer> buffers;
	private Comparator<MessageSearchResponse> responseComparator;
	
	public Th2DataProviderMessagesMerger(CommonFactory channel) {
		this.commonFactory = channel;
	}
	
	private void debugQueues() {
		StringBuilder sb = new StringBuilder();
		
		for (SingleStreamBuffer buffer : buffers) {
			sb.append("-")
			.append(buffer.getLoadedMessages());
			if (buffer.isCompletedWithError()) {
				sb.append(" [ERROR]");
			}
			if (buffer.isStreamCompleted()) {
				sb.append(" [FINISHED]");
			}
		}
		
		logger.debug("Queue sizes: {}", sb.toString());
	}
	
	public Iterator<MessageSearchResponse> searchMessages(List<MessageSearchRequest> searchOptions,
			Comparator<MessageSearchResponse> responseComparator) throws Exception {
		
		this.buffers = new ArrayList<>();
		this.responseComparator = responseComparator;
		
		for(MessageSearchRequest request : searchOptions) {
			DataProviderService client = commonFactory.getGrpcRouter().getService(DataProviderService.class);
			SingleStreamBuffer buffer = new SingleStreamBuffer(client.searchMessages(request));
			this.buffers.add(buffer);
		}

		//load initial values
		this.buffers.forEach(SingleStreamBuffer::next);
		
		return new MergeIterator();
	}
	
	private MessageSearchResponse getNextMessageBlocking() {
		
		try {

			SingleStreamBuffer nextBuffer = null;
			MessageSearchResponse nextResponse = null;

			for(SingleStreamBuffer buffer : buffers) {

				if(buffer.isCompletedWithError()) {
					throw new IllegalStateException("One of the message streams was closed with an error");
				}

				if (buffer.isStreamCompleted()) {
					continue;
				}

				MessageSearchResponse bufferMessage = buffer.getCurrentMessage();

				Objects.requireNonNull(buffer, "Should not be null. Can be null if stream is finished");
				if(nextResponse == null
						|| responseComparator.compare(bufferMessage, nextResponse) > 0) {
					nextResponse = bufferMessage;
					nextBuffer = buffer;
				}

			}

			if (logger.isDebugEnabled()) {
				this.debugQueues();
			}

			if(nextResponse != null) {
				nextBuffer.next();
				return nextResponse;
			}

			return null; // no more messages left
		
		} catch(Exception e) {
			throw new IllegalStateException("Unable to retrieve next message", e);
		}
		
	}
	
	private class MergeIterator implements Iterator<MessageSearchResponse> {
		
		private MessageSearchResponse next = null;
		
		@Override
		public boolean hasNext() {
			
			if(next != null) {
				return true;
			}
			
			next = getNextMessageBlocking();
			
			return next != null;
		}

		@Override
		public MessageSearchResponse next() {

			MessageSearchResponse result = null;
			
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
