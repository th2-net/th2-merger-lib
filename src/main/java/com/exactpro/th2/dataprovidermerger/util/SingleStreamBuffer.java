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

import com.exactpro.th2.dataprovider.grpc.MessageSearchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class SingleStreamBuffer {

    private static final Logger logger = LoggerFactory.getLogger(SingleStreamBuffer.class);

    private volatile boolean streamCompleted = false;
    private volatile boolean completedWithError = false;

    private final Iterator<MessageSearchResponse> sourceIterator;
    private MessageSearchResponse currentMessage;

    private int loadedMessages = 0;

    public SingleStreamBuffer(Iterator<MessageSearchResponse> sourceIterator) {
        this.sourceIterator = sourceIterator;
    }

    public void next() {
        try {
            if (this.sourceIterator.hasNext()) {
                this.currentMessage = this.sourceIterator.next();
                this.loadedMessages++;
            } else {
                this.currentMessage = null;
                this.streamCompleted = true;
            }
        } catch (Exception e) {
            completedWithError = true;
            streamCompleted = true;
            logger.error("Error on stream processing", e);
        }
    }

    public MessageSearchResponse getCurrentMessage() {
        return currentMessage;
    }

    public boolean isStreamCompleted() {
        return streamCompleted;
    }

    public boolean isCompletedWithError() {
        return completedWithError;
    }

    public int getLoadedMessages() {
        return loadedMessages;
    }
}
