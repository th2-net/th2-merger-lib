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

package com.exactpro.th2.dataprovidermerger.grpc;

import com.exactpro.th2.common.schema.factory.CommonFactory;
import com.exactpro.th2.dataprovider.grpc.DataProviderService;
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest;
import com.exactpro.th2.dataprovider.grpc.MessageSearchResponse;
import com.exactpro.th2.dataprovider.grpc.MessageStream;
import com.exactpro.th2.dataprovider.grpc.MessageStreamPointer;
import com.exactpro.th2.dataprovider.grpc.MessageStreamsRequest;
import com.exactpro.th2.dataprovider.grpc.MessageStreamsResponse;
import com.exactpro.th2.dataprovidermerger.Th2DataProviderMessagesMerger;
import com.exactpro.th2.dataprovidermerger.util.TimestampComparator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MergerServiceImpl extends MergerServiceStub implements DataProviderService {

    private final DataProviderService sourceService;
    private final Th2DataProviderMessagesMerger merger;


    public MergerServiceImpl(CommonFactory factory) throws MergerServiceException {
        try {
            this.sourceService = factory.getGrpcRouter().getService(DataProviderService.class);
        } catch (ClassNotFoundException e) {
            throw new MergerServiceException(e);
        }
        this.merger = new Th2DataProviderMessagesMerger(factory);
    }


    @Override
    public MessageStreamsResponse getMessageStreams(MessageStreamsRequest input) {
        return sourceService.getMessageStreams(input);
    }

    @Override
    public Iterator<MessageSearchResponse> searchMessages(MessageSearchRequest input) {
        try {
            return merger.searchMessages(buildRequestsFromSource(input), new TimestampComparator().reversed());
        } catch (Exception e) {
            throw new MergerServiceException(e);
        }
    }

    private List<MessageSearchRequest> buildRequestsFromSource(MessageSearchRequest source) {
        List<MessageSearchRequest> outList = new ArrayList<>();
        MessageSearchRequest.Builder builder = MessageSearchRequest.newBuilder();

        if (source.hasStartTimestamp()) {
            builder.setStartTimestamp(source.getStartTimestamp());
        }
        if (source.hasEndTimestamp()) {
            builder.setEndTimestamp(source.getEndTimestamp());
        }
        builder.setSearchDirection(source.getSearchDirection());
        if (source.hasResultCountLimit()) {
            builder.setResultCountLimit(source.getResultCountLimit());
        }
        if (source.getStreamPointerCount() > 0) {
            for (MessageStreamPointer messageID : source.getStreamPointerList()) {
                outList.add(builder.addStreamPointer(messageID).build());
                builder.clearStream();
            }
        } else if (source.getStreamCount() > 0) {
            for (MessageStream stream : source.getStreamList()) {
                outList.add(builder.addStream(stream).build());
                builder.clearStream();
            }
        }
        return outList;
    }
}
