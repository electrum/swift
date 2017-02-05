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
import com.facebook.swift.codec.internal.TProtocolReader;
import com.facebook.swift.codec.internal.TProtocolWriter;
import com.facebook.swift.codec.metadata.ThriftType;
import com.facebook.swift.transport.AddressSelector;
import com.facebook.swift.transport.ClientEventHandler;
import com.facebook.swift.transport.MethodInvoker;
import com.facebook.swift.transport.MethodMetadata;
import com.facebook.swift.transport.ParameterMetadata;
import com.facebook.swift.transport.SwiftApplicationException;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClient;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.async.TAsyncMethodCall;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TNonblockingTransport;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.apache.thrift.TApplicationException.BAD_SEQUENCE_ID;
import static org.apache.thrift.TApplicationException.INVALID_MESSAGE_TYPE;
import static org.apache.thrift.TApplicationException.WRONG_METHOD_NAME;
import static org.apache.thrift.protocol.TMessageType.CALL;
import static org.apache.thrift.protocol.TMessageType.EXCEPTION;
import static org.apache.thrift.protocol.TMessageType.REPLY;

public class ApacheThriftMethodInvoker
        implements MethodInvoker
{
    private final TAsyncClientManager asyncClientManager;
    private final AddressSelector addressSelector;
    private final TProtocolFactory protocolFactory;
    private final long timeout;

    public ApacheThriftMethodInvoker(TAsyncClientManager asyncClientManager, AddressSelector addressSelector, ApacheThriftClientConfig config)
    {
        this.asyncClientManager = requireNonNull(asyncClientManager, "asyncClientManager is null");
        this.addressSelector = requireNonNull(addressSelector, "addressSelector is null");
        requireNonNull(config, "config is null");
        this.protocolFactory = new TBinaryProtocol.Factory(false, true, config.getMaxStringSize(), config.getMaxFrameSize());
        timeout = config.getTimeout().toMillis();
    }

    @Override
    public ListenableFuture<Object> invoke(
            MethodMetadata method,
            List<ClientEventHandler<?>> handlers,
            Optional<String> addressSelectionContext,
            Map<String, String> headers,
            List<Object> parameters)
    {
        try {
            List<HostAndPort> addresses = addressSelector.getAddresses(addressSelectionContext);
            InvocationAttempt invocationAttempt = new InvocationAttempt(
                    addresses,
                    connection -> invoke(connection, handlers, method, parameters),
                    addressSelector::markdown);
            return invocationAttempt.getFuture();
        }
        catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }
    }

    private ListenableFuture<Object> invoke(TNonblockingTransport transport, List<ClientEventHandler<?>> handlers, MethodMetadata method, List<Object> parameters)
    {
        try {
            AsyncClient asyncClient = new AsyncClient(method, protocolFactory, asyncClientManager, transport, timeout);
            return asyncClient.execute(parameters);
        }
        catch (Throwable throwable) {
            if (throwable instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                throwable = new TException(throwable);
            }
            return Futures.immediateFailedFuture(throwable);
        }
    }

    private static class AsyncClient
            extends TAsyncClient
    {
        private final MethodMetadata method;

        private AsyncClient(MethodMetadata method, TProtocolFactory protocolFactory, TAsyncClientManager manager, TNonblockingTransport transport, long timeout)
        {
            super(protocolFactory, manager, transport, timeout);
            this.method = method;
        }

        private ListenableFuture<Object> execute(List<Object> parameters)
                throws Exception
        {
            SettableFuture<Object> futureResult = SettableFuture.create();
            ___manager.call(new MethodCall(
                    method,
                    parameters,
                    this,
                    ___transport,
                    new AsyncMethodCallback<MethodCall>()
                    {
                        @Override
                        public void onComplete(MethodCall response)
                        {
                            try {
                                futureResult.set(response.getResult());
                            }
                            catch (Throwable e) {
                                futureResult.setException(e);
                            }
                        }

                        @Override
                        public void onError(Exception exception)
                        {
                            futureResult.setException(exception);
                        }
                    },
                    method.isOneway()));
            return futureResult;
        }
    }

    public static class MethodCall
            extends TAsyncMethodCall<MethodCall>
    {
        // Thrift client always uses sequence id 0, since it is not thread safe
        private static final int SEQUENCE_ID = 0;

        private final MethodMetadata method;
        private final List<Object> parameters;

        private MethodCall(
                MethodMetadata method,
                List<Object> parameters,
                TAsyncClient client,
                TNonblockingTransport transport,
                AsyncMethodCallback<MethodCall> callback,
                boolean isOneway)
        {
            super(client, client.getProtocolFactory(), transport, callback, isOneway);
            this.method = method;
            this.parameters = parameters;
        }

        public void write_args(TProtocol requestProtocol)
                throws TException
        {
            try {
                // Thrift client always uses CALL and always uses 0 for the sequence id (since it is not thread safe)
                requestProtocol.writeMessageBegin(new TMessage(method.getName(), CALL, SEQUENCE_ID));

                // write the parameters
                TProtocolWriter writer = new TProtocolWriter(requestProtocol);
                writer.writeStructBegin(method.getName() + "_args");
                for (int i = 0; i < parameters.size(); i++) {
                    Object value = parameters.get(i);
                    ParameterMetadata parameter = method.getParameters().get(i);
                    writer.writeField(parameter.getName(), parameter.getId(), parameter.getCodec(), value);
                }
                writer.writeStructEnd();

                requestProtocol.writeMessageEnd();
            }
            catch (TException e) {
                throw e;
            }
            catch (Exception e) {
                throw new TException(e);
            }
        }

        private Object getResult()
                throws TException
        {
            if (getState() != State.RESPONSE_READ) {
                throw new IllegalStateException("Method call not finished!");
            }
            TProtocol responseProtocol = client.getProtocolFactory().getProtocol(new TMemoryInputTransport(getFrameBuffer().array()));

            // validate response header
            TMessage message = responseProtocol.readMessageBegin();
            if (message.type == EXCEPTION) {
                TApplicationException exception = TApplicationException.read(responseProtocol);
                responseProtocol.readMessageEnd();
                throw exception;
            }
            if (message.type != REPLY) {
                throw new TApplicationException(INVALID_MESSAGE_TYPE, "Received invalid message type " + message.type + " from server");
            }
            if (!message.name.equals(method.getName())) {
                throw new TApplicationException(WRONG_METHOD_NAME, "Wrong method name in reply: expected " + method.getName() + " but received " + message.name);
            }
            if (message.seqid != SEQUENCE_ID) {
                throw new TApplicationException(BAD_SEQUENCE_ID, method.getName() + " failed: out of sequence response");
            }

            // read response struct
            TProtocolReader reader = new TProtocolReader(responseProtocol);
            reader.readStructBegin();

            Object results = null;
            Exception exception = null;
            try {
                while (reader.nextField()) {
                    if (reader.getFieldId() == 0) {
                        results = reader.readField(method.getResultCodec());
                    }
                    else {
                        ThriftCodec<Object> exceptionCodec = method.getExceptionCodecs().get(reader.getFieldId());
                        if (exceptionCodec != null) {
                            exception = (Exception) reader.readField(exceptionCodec);
                        }
                        else {
                            reader.skipFieldData();
                        }
                    }
                }
                reader.readStructEnd();
                responseProtocol.readMessageEnd();
            }
            catch (TException e) {
                throw e;
            }
            catch (Exception e) {
                throw new TException(e);
            }

            if (exception != null) {
                throw new SwiftApplicationException(exception);
            }

            if (method.getResultCodec().getType() == ThriftType.VOID) {
                return null;
            }

            if (results == null) {
                throw new TApplicationException(TApplicationException.MISSING_RESULT, method.getName() + " failed: unknown result");
            }
            return results;
        }
    }
}
