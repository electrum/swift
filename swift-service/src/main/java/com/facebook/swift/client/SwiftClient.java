package com.facebook.swift.client;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

public interface SwiftClient<T>
{
    default T get()
    {
        return get(Optional.empty());
    }

    default T get(Optional<String> addressSelectionContext)
    {
        return get(addressSelectionContext, ImmutableMap.of());
    }

    default T get(Map<String, String> headers)
    {
        return get(Optional.empty(), headers);
    }

    T get(Optional<String> addressSelectionContext, Map<String, String> headers);
}
