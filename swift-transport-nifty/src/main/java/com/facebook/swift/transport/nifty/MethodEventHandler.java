/*
 * Copyright (C) 2013 Facebook, Inc.
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
package com.facebook.swift.transport.nifty;

import com.facebook.swift.transport.ClientEventHandler;
import com.facebook.swift.transport.ConnectionContext;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class MethodEventHandler
{
    private final List<ClientEventHandlerAdapter<?>> handlers;

    public MethodEventHandler(List<? extends ClientEventHandler<?>> handlers, String methodName, ConnectionContext connectionContext)
    {
        this.handlers = ImmutableList.copyOf(handlers.stream()
                .map(handler -> new ClientEventHandlerAdapter<>(methodName, handler, connectionContext))
                .collect(Collectors.toList()));
    }

    public void preWrite(List<Object> parameters)
    {
        for (ClientEventHandlerAdapter<?> handler : handlers) {
            handler.preWrite(parameters);
        }
    }

    public void postWrite(List<Object> parameters)
    {
        for (ClientEventHandlerAdapter<?> handler : handlers) {
            handler.postWrite(parameters);
        }
    }

    public void preRead()
    {
        for (ClientEventHandlerAdapter<?> handler : handlers) {
            handler.preRead();
        }
    }

    public void postRead(Object result)
    {
        for (ClientEventHandlerAdapter<?> handler : handlers) {
            handler.postRead(result);
        }
    }

    public void postReadException(Throwable t)
    {
        for (ClientEventHandlerAdapter<?> handler : handlers) {
            handler.postReadException(t);
        }
    }

    public void done()
    {
        for (ClientEventHandlerAdapter<?> handler : handlers) {
            handler.done();
        }
    }

    private static class ClientEventHandlerAdapter<T>
    {
        private final ClientEventHandler<T> handler;
        private final String methodName;
        private final T context;

        private ClientEventHandlerAdapter(String methodName, ClientEventHandler<T> handler, ConnectionContext connectionContext)
        {
            this.methodName = requireNonNull(methodName, "methodName is null");
            this.handler = requireNonNull(handler, "handler is null");
            this.context = handler.getContext(methodName, requireNonNull(connectionContext, "connectionContext is null"));
        }

        private void preWrite(List<Object> parameters)
        {
            handler.preWrite(context, methodName, parameters);
        }

        private void postWrite(List<Object> parameters)
        {
            handler.postWrite(context, methodName, parameters);
        }

        private void preRead()
        {
            handler.preRead(context, methodName);
        }

        private void postRead(Object result)
        {
            handler.postRead(context, methodName, result);
        }

        private void postReadException(Throwable t)
        {
            handler.postReadException(context, methodName, t);
        }

        private void done()
        {
            handler.done(context, methodName);
        }
    }
}
