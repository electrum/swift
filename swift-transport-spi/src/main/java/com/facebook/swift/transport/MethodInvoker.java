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
package com.facebook.swift.transport;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface MethodInvoker
{
    /**
     * Invoke the specified method asynchronously.
     *
     * If the invocation fails with a known application exception, the future will contain a
     * SwiftApplicationException wrapper; otherwise the future will contain the raw transport
     * exception.
     */
    ListenableFuture<Object> invoke(
            MethodMetadata method,
            List<ClientEventHandler<?>> handlers,
            Optional<String> addressSelectionContext,
            Map<String, String> headers,
            List<Object> parameters);
}
