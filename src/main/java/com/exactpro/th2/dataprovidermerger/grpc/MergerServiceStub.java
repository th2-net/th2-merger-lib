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

import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.dataprovider.grpc.BulkEventRequest;
import com.exactpro.th2.dataprovider.grpc.BulkEventResponse;
import com.exactpro.th2.dataprovider.grpc.DataProviderService;
import com.exactpro.th2.dataprovider.grpc.EventFiltersRequest;
import com.exactpro.th2.dataprovider.grpc.EventMatchRequest;
import com.exactpro.th2.dataprovider.grpc.EventResponse;
import com.exactpro.th2.dataprovider.grpc.EventSearchRequest;
import com.exactpro.th2.dataprovider.grpc.EventSearchResponse;
import com.exactpro.th2.dataprovider.grpc.FilterInfoRequest;
import com.exactpro.th2.dataprovider.grpc.FilterInfoResponse;
import com.exactpro.th2.dataprovider.grpc.FilterNamesResponse;
import com.exactpro.th2.dataprovider.grpc.MatchResponse;
import com.exactpro.th2.dataprovider.grpc.MessageFiltersRequest;
import com.exactpro.th2.dataprovider.grpc.MessageGroupResponse;
import com.exactpro.th2.dataprovider.grpc.MessageMatchRequest;
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest;
import com.exactpro.th2.dataprovider.grpc.MessageSearchResponse;
import com.exactpro.th2.dataprovider.grpc.MessageStreamsRequest;
import com.exactpro.th2.dataprovider.grpc.MessageStreamsResponse;
import org.apache.commons.lang3.NotImplementedException;

import java.util.Iterator;

public class MergerServiceStub implements DataProviderService {


    @Override
    public EventResponse getEvent(EventID input) {
        throw new NotImplementedException("Method is not supported");
    }

    @Override
    public BulkEventResponse getEvents(BulkEventRequest input) {
        throw new NotImplementedException("Method is not supported");
    }

    @Override
    public MessageGroupResponse getMessage(MessageID input) {
        throw new NotImplementedException("Method is not supported");
    }

    @Override
    public MessageStreamsResponse getMessageStreams(MessageStreamsRequest input) {
        throw new NotImplementedException("Method is not supported");
    }

    @Override
    public Iterator<MessageSearchResponse> searchMessages(MessageSearchRequest input) {
        throw new NotImplementedException("Method is not supported");
    }

    @Override
    public Iterator<EventSearchResponse> searchEvents(EventSearchRequest input) {
        throw new NotImplementedException("Method is not supported");
    }

    @Override
    public FilterNamesResponse getMessagesFilters(MessageFiltersRequest input) {
        throw new NotImplementedException("Method is not supported");
    }

    @Override
    public FilterNamesResponse getEventsFilters(EventFiltersRequest input) {
        throw new NotImplementedException("Method is not supported");
    }

    @Override
    public FilterInfoResponse getEventFilterInfo(FilterInfoRequest input) {
        throw new NotImplementedException("Method is not supported");
    }

    @Override
    public FilterInfoResponse getMessageFilterInfo(FilterInfoRequest input) {
        throw new NotImplementedException("Method is not supported");
    }

    @Override
    public MatchResponse matchEvent(EventMatchRequest input) {
        throw new NotImplementedException("Method is not supported");
    }

    @Override
    public MatchResponse matchMessage(MessageMatchRequest input) {
        throw new NotImplementedException("Method is not supported");
    }
}
