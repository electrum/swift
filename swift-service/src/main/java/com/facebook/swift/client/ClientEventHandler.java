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

import java.util.List;

public interface ClientEventHandler<T>
{
    default T getContext(String methodName, ConnectionContext context)
    {
        return null;
    }

    default void preWrite(T context, String methodName, List<Object> parameters) {}

    default void postWrite(T context, String methodName, List<Object> parameters) {}

    default void preRead(T context, String methodName) {}

    default void postRead(T context, String methodName, Object result) {}

    default void postReadException(T context, String methodName, Throwable t) {}

    default void done(T context, String methodName) {}
}
