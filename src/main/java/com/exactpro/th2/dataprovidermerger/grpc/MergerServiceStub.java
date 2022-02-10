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
import com.exactpro.th2.dataprovider.grpc.DataProviderService;
import com.exactpro.th2.dataprovider.grpc.EventData;
import com.exactpro.th2.dataprovider.grpc.EventIds;
import com.exactpro.th2.dataprovider.grpc.EventSearchRequest;
import com.exactpro.th2.dataprovider.grpc.Events;
import com.exactpro.th2.dataprovider.grpc.FilterInfo;
import com.exactpro.th2.dataprovider.grpc.FilterName;
import com.exactpro.th2.dataprovider.grpc.IsMatched;
import com.exactpro.th2.dataprovider.grpc.ListFilterName;
import com.exactpro.th2.dataprovider.grpc.MatchRequest;
import com.exactpro.th2.dataprovider.grpc.MessageData;
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest;
import com.exactpro.th2.dataprovider.grpc.StreamResponse;
import com.exactpro.th2.dataprovider.grpc.StringList;
import com.google.protobuf.Empty;
import org.apache.commons.lang3.NotImplementedException;

import java.util.Iterator;

public class MergerServiceStub implements DataProviderService {

    @Override
    public EventData getEvent(EventID input) {
        throw new NotImplementedException("Method is not supported");
    }

    @Override
    public Events getEvents(EventIds input) {
        throw new NotImplementedException("Method is not supported");
    }

    @Override
    public MessageData getMessage(MessageID input) {
        throw new NotImplementedException("Method is not supported");
    }

    @Override
    public StringList getMessageStreams(Empty input) {
        throw new NotImplementedException("Method is not supported");
    }

    @Override
    public Iterator<StreamResponse> searchMessages(MessageSearchRequest input) {
        throw new NotImplementedException("Method is not supported");
    }

    @Override
    public Iterator<StreamResponse> searchEvents(EventSearchRequest input) {
        throw new NotImplementedException("Method is not supported");
    }

    @Override
    public ListFilterName getMessagesFilters(Empty input) {
        throw new NotImplementedException("Method is not supported");
    }

    @Override
    public ListFilterName getEventsFilters(Empty input) {
        throw new NotImplementedException("Method is not supported");
    }

    @Override
    public FilterInfo getEventFilterInfo(FilterName input) {
        throw new NotImplementedException("Method is not supported");
    }

    @Override
    public FilterInfo getMessageFilterInfo(FilterName input) {
        throw new NotImplementedException("Method is not supported");
    }

    @Override
    public IsMatched matchEvent(MatchRequest input) {
        throw new NotImplementedException("Method is not supported");
    }

    @Override
    public IsMatched matchMessage(MatchRequest input) {
        throw new NotImplementedException("Method is not supported");
    }
}
