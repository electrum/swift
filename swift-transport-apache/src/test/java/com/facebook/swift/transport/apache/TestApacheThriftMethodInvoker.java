/*
 * Copyright (C) 2012 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.facebook.swift.transport.apache;

import com.facebook.swift.codec.ThriftCodec;
import com.facebook.swift.codec.ThriftCodecManager;
import com.facebook.swift.transport.AddressSelector;
import com.facebook.swift.transport.ClientEventHandler;
import com.facebook.swift.transport.ConnectionContext;
import com.facebook.swift.transport.MethodMetadata;
import com.facebook.swift.transport.ParameterMetadata;
import com.facebook.swift.transport.apache.scribe.apache.LogEntry;
import com.facebook.swift.transport.apache.scribe.apache.ResultCode;
import com.facebook.swift.transport.apache.scribe.apache.ScribeService;
import com.facebook.swift.transport.apache.scribe.apache.scribe;
import com.facebook.swift.transport.apache.scribe.apache.scribe.AsyncClient.Log_call;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

import static com.facebook.swift.codec.metadata.ThriftType.list;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.nCopies;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestApacheThriftMethodInvoker
{
    private static final ThriftCodecManager codecManager = new ThriftCodecManager();
    private static final List<LogEntry> MESSAGES = ImmutableList.of(
            new LogEntry("hello", "world"),
            new LogEntry("bye", "world"));
    private static final List<com.facebook.swift.transport.apache.scribe.swift.LogEntry> SWIFT_MESSAGES = ImmutableList.copyOf(
            MESSAGES.stream()
                    .map(input -> new com.facebook.swift.transport.apache.scribe.swift.LogEntry(input.category, input.message))
                    .collect(Collectors.toList()));
    private static final com.facebook.swift.transport.apache.scribe.swift.ResultCode SWIFT_OK = com.facebook.swift.transport.apache.scribe.swift.ResultCode.OK;

    @Test
    public void testThriftService()
            throws Exception
    {
        ScribeService scribeService = new ScribeService();
        TProcessor processor = new scribe.Processor<>(scribeService);

        List<LogEntry> expectedMessages = testProcessor(processor);
        Assert.assertEquals(scribeService.getMessages(), expectedMessages);
    }

    private List<LogEntry> testProcessor(TProcessor processor)
            throws Exception
    {
        int invocationCount = testProcessor(processor, ImmutableList.of(
                address -> logThrift(address, MESSAGES),
                address -> logThriftAsync(address, MESSAGES),
                address -> logApacheThriftInvocationHandler(address, SWIFT_MESSAGES, ImmutableList.of())));

        return newArrayList(Iterables.concat(nCopies(invocationCount, MESSAGES)));
    }

    private int testProcessor(TProcessor processor, List<ToIntFunction<HostAndPort>> clients)
            throws Exception
    {
        try (
                TServerSocket serverTransport = new TServerSocket(0)
        ) {
            TProtocolFactory protocolFactory = new Factory();
            TTransportFactory transportFactory = new TFramedTransport.Factory();
            TServer server = new TSimpleServer(new Args(serverTransport)
                    .protocolFactory(protocolFactory)
                    .transportFactory(transportFactory)
                    .processor(processor));

            Thread serverThread = new Thread(server::serve);
            try {
                serverThread.start();

                int localPort = serverTransport.getServerSocket().getLocalPort();
                HostAndPort address = HostAndPort.fromParts("localhost", localPort);

                int sum = 0;
                for (ToIntFunction<HostAndPort> client : clients) {
                    sum += client.applyAsInt(address);
                }
                return sum;
            }
            finally {
                server.stop();
                serverThread.interrupt();
            }
        }
    }

    private int logThrift(HostAndPort address, List<LogEntry> messages)
    {
        try {
            TSocket socket = new TSocket(address.getHostText(), address.getPort());
            socket.open();
            try {
                TBinaryProtocol tp = new TBinaryProtocol(new TFramedTransport(socket));
                Assert.assertEquals(new scribe.Client(tp).Log(messages), ResultCode.OK);
            }
            finally {
                socket.close();
            }
        }
        catch (TException e) {
            throw Throwables.propagate(e);
        }
        return 1;
    }

    private int logThriftAsync(HostAndPort address, List<LogEntry> messages)
    {
        try {
            TAsyncClientManager asyncClientManager = new TAsyncClientManager();
            try (TNonblockingSocket socket = new TNonblockingSocket(address.getHostText(), address.getPort())) {

                scribe.AsyncClient client = new scribe.AsyncClient(
                        new Factory(),
                        asyncClientManager,
                        socket);

                SettableFuture<ResultCode> futureResult = SettableFuture.create();
                client.Log(messages, new AsyncMethodCallback<Log_call>()
                {
                    @Override
                    public void onComplete(Log_call response)
                    {
                        try {
                            futureResult.set(response.getResult());
                        }
                        catch (Throwable exception) {
                            futureResult.setException(exception);
                        }
                    }

                    @Override
                    public void onError(Exception exception)
                    {
                        futureResult.setException(exception);
                    }
                });
                Assert.assertEquals(futureResult.get(), ResultCode.OK);
            }
            finally {
                asyncClientManager.stop();
            }
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
        return 1;
    }

    private int logApacheThriftInvocationHandler(HostAndPort address, List<com.facebook.swift.transport.apache.scribe.swift.LogEntry> entries, List<ClientEventHandler<?>> handlers)
    {
        AddressSelector addressSelector = context -> ImmutableList.of(address);
        ApacheThriftClientConfig config = new ApacheThriftClientConfig();
        TAsyncClientManager asyncClientManager = null;
        try {
            asyncClientManager = new TAsyncClientManager();
            ApacheThriftMethodInvoker methodInvoker = new ApacheThriftMethodInvoker(asyncClientManager, addressSelector, config);

            ParameterMetadata parameter = new ParameterMetadata(
                    (short) 1,
                    "messages",
                    (ThriftCodec<Object>) codecManager.getCodec(list(codecManager.getCodec(com.facebook.swift.transport.apache.scribe.swift.LogEntry.class).getType())));

            MethodMetadata methodMetadata = new MethodMetadata(
                    "Log",
                    ImmutableList.of(parameter),
                    (ThriftCodec<Object>) (Object) codecManager.getCodec(com.facebook.swift.transport.apache.scribe.swift.ResultCode.class),
                    ImmutableMap.of(), false);

            ListenableFuture<Object> future = methodInvoker.invoke(methodMetadata, handlers, Optional.empty(), ImmutableMap.of(), ImmutableList.of(entries));
            Assert.assertEquals(future.get(), SWIFT_OK);

            return 1;
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
        finally {
            if (asyncClientManager != null) {
                asyncClientManager.stop();
            }
        }
    }

    @Test
    public void testSwiftEventHandlers()
            throws Exception
    {
        ScribeService scribeService = new ScribeService();
        TProcessor processor = new scribe.Processor<>(scribeService);

        EventHandler eventHandler = new EventHandler();
        EventHandler secondHandler = new EventHandler();
        List<ClientEventHandler<?>> handlers = ImmutableList.of(eventHandler, secondHandler);

        testProcessor(processor, ImmutableList.of(
                address -> logApacheThriftInvocationHandler(address, SWIFT_MESSAGES, handlers),
                address -> logApacheThriftInvocationHandler(address, SWIFT_MESSAGES, handlers)));

        Assert.assertEquals(scribeService.getMessages(), Lists.newArrayList(Iterables.concat(MESSAGES, MESSAGES)));
//        eventHandler.assertCounts(2);
//        secondHandler.assertCounts(2);
    }

    static class EventHandler
            implements ClientEventHandler<EventHandler.EventContext>
    {
        static class EventContext
        {
            private final String methodName;

            EventContext(String methodName)
            {
                this.methodName = methodName;
            }
        }

        private int getContextCounter = 0;
        private int preWriteCounter = 0;
        private int postWriteCounter = 0;
        private int preReadCounter = 0;
        private int postReadCounter = 0;
        private int doneCounter = 0;

        private final Set<Object> contexts = new LinkedHashSet<>();

        void assertCounts(int count)
        {
            assertEquals(getContextCounter, count, "getContextCounter");
            assertEquals(preWriteCounter, count, "preWriteCounter");
            assertEquals(postWriteCounter, count, "postWriteCounter");
            assertEquals(preReadCounter, count, "preReadCounter");
            assertEquals(postReadCounter, count, "postReadCounter");
            assertEquals(doneCounter, count, "doneCounter");
        }

        @Override
        public EventContext getContext(String methodName, ConnectionContext context)
        {
            assertEquals(methodName, "Log");
            assertNotNull(context);
            assertTrue(((InetSocketAddress) context.getRemoteAddress()).getAddress().isLoopbackAddress());

            EventContext eventContext = new EventContext(methodName);
            contexts.add(eventContext);
            getContextCounter++;
            return eventContext;
        }

        @Override
        public void preWrite(EventContext context, String methodName, List<Object> parameters)
        {
            preWriteCounter++;
            assertEquals(methodName, "Log");
            assertEquals(context.methodName, "Log");
            assertTrue(contexts.contains(context));
            assertEquals(parameters.size(), 1);
            assertTrue(parameters.get(0) instanceof List);
        }

        @Override
        public void postWrite(EventContext context, String methodName, List<Object> parameters)
        {
            postWriteCounter++;
            assertEquals(methodName, "Log");
            assertEquals(context.methodName, "Log");
            assertTrue(contexts.contains(context));
            assertEquals(parameters.size(), 1);
            assertTrue(parameters.get(0) instanceof List);
        }

        @Override
        public void preRead(EventContext context, String methodName)
        {
            preReadCounter++;
            assertEquals(methodName, "Log");
            assertEquals(context.methodName, "Log");
            assertTrue(contexts.contains(context));
        }

        @Override
        public void postRead(EventContext context, String methodName, Object result)
        {
            postReadCounter++;
            assertEquals(methodName, "Log");
            assertEquals(context.methodName, "Log");
            assertTrue(contexts.contains(context));
            assertTrue(result instanceof com.facebook.swift.transport.apache.scribe.swift.ResultCode);
        }

        @Override
        public void postReadException(EventContext context, String methodName, Throwable t)
        {
            fail("Did not expect to read an exception");
        }

        @Override
        public void done(EventContext context, String methodName)
        {
            doneCounter++;
            assertEquals(methodName, "Log");
            assertEquals(context.methodName, "Log");
            assertTrue(contexts.contains(context));
        }
    }
}
