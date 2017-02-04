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
package com.facebook.swift.client;

import com.facebook.swift.transport.ClientEventHandler;
import com.facebook.swift.transport.MethodInvoker;
import com.facebook.swift.transport.MethodMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

class SwiftMethodHandler
{
    private final MethodMetadata metadata;
    private final MethodInvoker invoker;
    private final boolean async;
    private final List<ClientEventHandler<?>> handlers;

    public SwiftMethodHandler(MethodMetadata metadata, MethodInvoker invoker, List<ClientEventHandler<?>> handlers, boolean async)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.invoker = requireNonNull(invoker, "invoker is null");
        this.handlers = ImmutableList.copyOf(requireNonNull(handlers, "handlers is null"));
        this.async = async;
    }

    public boolean isAsync()
    {
        return async;
    }

    public ListenableFuture<Object> invoke(Optional<String> addressSelectionContext, Map<String, String> headers, List<Object> args)
    {
        return invoker.invoke(metadata, handlers, addressSelectionContext, headers, args);
    }
}
