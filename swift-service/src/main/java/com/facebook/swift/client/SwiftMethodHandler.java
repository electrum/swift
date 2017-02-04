package com.facebook.swift.client;

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
