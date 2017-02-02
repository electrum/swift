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

import javax.inject.Qualifier;

import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Objects;

import static java.lang.String.format;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

public final class SwiftClientAnnotationFactory
{
    private SwiftClientAnnotationFactory() {}

    public static Annotation getSwiftClientAnnotation(Class<?> value, Class<? extends Annotation> qualifier)
    {
        return new SwiftClientAnnotationImpl(value, qualifier);
    }

    @Target({FIELD, PARAMETER, METHOD})
    @Retention(RUNTIME)
    @Qualifier
    @interface SwiftClientAnnotation
    {
        Class<?> value();

        Class<? extends Annotation> qualifier();
    }

    @SuppressWarnings("ClassExplicitlyAnnotation")
    private static final class SwiftClientAnnotationImpl
            implements SwiftClientAnnotation
    {
        private final Class<?> value;
        private final Class<? extends Annotation> qualifier;

        private SwiftClientAnnotationImpl(Class<?> value, Class<? extends Annotation> qualifier)
        {
            this.value = requireNonNull(value, "value is null");
            this.qualifier = requireNonNull(qualifier, "qualifier is null");
        }

        @Override
        public Class<?> value()
        {
            return value;
        }

        @Override
        public Class<? extends Annotation> qualifier()
        {
            return qualifier;
        }

        @Override
        public Class<? extends Annotation> annotationType()
        {
            return SwiftClientAnnotation.class;
        }

        public int hashCode()
        {
            // This is specified in java.lang.Annotation.
            return (127 * "value".hashCode()) ^ value.hashCode() +
                    (127 * "qualifier".hashCode()) ^ qualifier.hashCode();
        }

        public boolean equals(Object o)
        {
            if (!(o instanceof SwiftClientAnnotation)) {
                return false;
            }
            SwiftClientAnnotation other = (SwiftClientAnnotation) o;
            return Objects.equals(value, other.value()) && Objects.equals(qualifier, other.qualifier());
        }

        @Override
        public String toString()
        {
            return format("@%s(value=%s, qualifier=%s)", SwiftClientAnnotation.class.getName(), value, qualifier);
        }
    }
}
