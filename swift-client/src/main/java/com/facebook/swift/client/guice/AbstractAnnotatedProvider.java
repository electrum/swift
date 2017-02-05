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
package com.facebook.swift.client.guice;

import com.google.inject.Injector;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.annotation.Annotation;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

abstract class AbstractAnnotatedProvider<C, T>
        implements Provider<T>
{
    private final Class<? extends Annotation> annotation;
    private final Class<C> type;
    private Injector injector;

    protected AbstractAnnotatedProvider(Class<C> type, Class<? extends Annotation> annotation)
    {
        this.annotation = requireNonNull(annotation, "annotation is null");
        this.type = requireNonNull(type, "type is null");
    }

    @Inject
    public final void setInjector(Injector injector)
    {
        this.injector = injector;
    }

    @Override
    public final T get()
    {
        checkState(injector != null, "injector was not set");
        return get(injector, type, annotation);
    }

    protected abstract T get(Injector injector, Class<C> type, Class<? extends Annotation> annotation);

    @Override
    public final boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        AbstractAnnotatedProvider<?, ?> other = (AbstractAnnotatedProvider<?, ?>) obj;
        return Objects.equals(this.type, other.type) &&
                Objects.equals(this.annotation, other.annotation);
    }

    @Override
    public final int hashCode()
    {
        return Objects.hash(type, annotation);
    }
}
