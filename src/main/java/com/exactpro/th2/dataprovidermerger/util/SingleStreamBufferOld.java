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

package com.exactpro.th2.dataprovidermerger.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.exactpro.th2.dataprovider.grpc.MessageSearchResponse;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Deprecated
public class SingleStreamBufferOld implements StreamObserver<MessageSearchResponse> {
	
	private static final Logger logger = LoggerFactory.getLogger(SingleStreamBuffer.class);
	
	private BlockingQueue<MessageSearchResponse> queue = new LinkedBlockingQueue<>();

	private volatile boolean streamCompleted = false;
	
	private volatile boolean completedWithError = false;
	
	public SingleStreamBufferOld(Integer messageQueueSizeLimit) {
		
		if(messageQueueSizeLimit != null) {
			queue = new LinkedBlockingQueue<>(messageQueueSizeLimit);
		} else {
			queue = new LinkedBlockingQueue<>();
		}
		
	}
	
	@Override
	public void onNext(MessageSearchResponse value) {
		
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
	
	public BlockingQueue<MessageSearchResponse> getQueue() {
		return queue;
	}

	public boolean isStreamCompleted() {
		return streamCompleted;
	}

	public boolean isCompletedWithError() {
		return completedWithError;
	}
	
}
