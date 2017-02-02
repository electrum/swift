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
package com.facebook.swift.client.nifty;

import com.facebook.swift.service.Scribe;
import com.facebook.swift.service.ThriftClientConfig;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.airlift.bootstrap.Bootstrap;
import org.testng.annotations.Test;

import javax.inject.Qualifier;

import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static com.facebook.swift.client.guice.SwiftClientAnnotationFactory.getSwiftClientAnnotation;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static org.testng.Assert.assertNotNull;

public class TestNiftyClientModule
{
    @Test
    public void test()
            throws Exception
    {
        Annotation clientAnnotation = getSwiftClientAnnotation(Scribe.class, TestQualifiedAnnotation.class);
        Bootstrap bootstrap = new Bootstrap(
                new NiftyClientModule(),
                binder -> {
                    configBinder(binder).bindConfig(ThriftClientConfig.class, clientAnnotation);
                });


        Injector injector = bootstrap
                .doNotInitializeLogging()
                .strictConfig()
                .initialize();

        ThriftClientConfig clientConfig = injector.getInstance(Key.get(ThriftClientConfig.class, clientAnnotation));
        NiftyClientConfig niftyClientConfig = injector.getInstance(Key.get(NiftyClientConfig.class, clientAnnotation));
        assertNotNull(clientConfig);
    }

    @Target({FIELD, PARAMETER, METHOD})
    @Retention(RUNTIME)
    @Qualifier
    @interface TestQualifiedAnnotation
    {
    }
}
