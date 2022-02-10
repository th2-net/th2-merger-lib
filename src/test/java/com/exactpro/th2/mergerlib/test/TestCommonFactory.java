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

package com.exactpro.th2.mergerlib.test;

import com.exactpro.th2.common.schema.factory.CommonFactory;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcConfiguration;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcRetryConfiguration;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcRouterConfiguration;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcServiceConfiguration;
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter;
import com.exactpro.th2.common.schema.grpc.router.impl.DefaultStubStorage;
import com.exactpro.th2.common.schema.strategy.route.impl.RobinRoutingStrategy;
import com.exactpro.th2.dataprovider.grpc.DataProviderDefaultBlockingImpl;
import com.exactpro.th2.dataprovider.grpc.DataProviderGrpc;
import com.exactpro.th2.dataprovider.grpc.DataProviderService;
import com.google.protobuf.Message;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.commons.lang3.NotImplementedException;

import java.util.Collections;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class TestCommonFactory extends CommonFactory {

    private final String serverName;
    private final Executor executors;

    private ManagedChannel channel;

    public TestCommonFactory(String serverName, Executor executors) {
        this.serverName = serverName;
        this.executors = executors;

        channel = InProcessChannelBuilder
                .forName(serverName)
                .executor(executors)
                .build();
    }

    @Override
    public GrpcRouter getGrpcRouter() {
        return new GrpcRouterTest();
    }

    private class GrpcRouterTest implements GrpcRouter {

        @Override
        public void init(GrpcRouterConfiguration configuration) {
            throw new NotImplementedException("Not implemented for test");
        }

        @Override
        public void init(GrpcConfiguration configuration, GrpcRouterConfiguration routerConfiguration) {
            throw new NotImplementedException("Not implemented for test");
        }

        @Override
        public <T> T getService(Class<T> cls) throws ClassNotFoundException {
            if (DataProviderService.class == cls) {
                return (T) new DaraProviderServiceImpl2(channel);
            }
            throw new ClassNotFoundException();
        }

        @Override
        public Server startServer(BindableService... services) {
            throw new NotImplementedException("Not implemented for test");
        }

        @Override
        public void close() throws Exception {

        }
    }

    private static class DaraProviderServiceImpl2 extends DataProviderDefaultBlockingImpl {

        private final ManagedChannel channel;

        public DaraProviderServiceImpl2(ManagedChannel channel) {
            super(new GrpcRetryConfiguration(), new DefaultStubStorage<>(new GrpcServiceConfiguration(new RobinRoutingStrategy(), Class.class, Collections.emptyMap())));
            this.channel = channel;
        }

        @Override
        protected DataProviderGrpc.DataProviderBlockingStub getStub(Message message) {
            return DataProviderGrpc.newBlockingStub(channel);
        }
    }
}
