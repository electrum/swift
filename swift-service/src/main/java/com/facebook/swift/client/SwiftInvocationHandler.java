package com.facebook.swift.client;

import com.facebook.swift.service.RuntimeTApplicationException;
import com.facebook.swift.service.RuntimeTException;
import com.facebook.swift.service.RuntimeTProtocolException;
import com.facebook.swift.service.RuntimeTTransportException;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransportException;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.apache.thrift.TApplicationException.UNKNOWN_METHOD;

class SwiftInvocationHandler
        implements InvocationHandler
{
    private static final Object[] NO_ARGS = new Object[0];

    private final Map<Method, SwiftMethodHandler> methods;
    private final Optional<String> addressSelectionContext;
    private final Map<String, String> headers;

    public SwiftInvocationHandler(
            Map<Method, SwiftMethodHandler> methods,
            Optional<String> addressSelectionContext,
            Map<String, String> headers)
    {
        this.methods = ImmutableMap.copyOf(requireNonNull(methods, "methods is null"));
        this.addressSelectionContext = requireNonNull(addressSelectionContext, "addressSelectionContext is null");
        this.headers = ImmutableMap.copyOf(requireNonNull(headers, "headers is null"));
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable
    {
        if (method.getDeclaringClass() == Object.class) {
            switch (method.getName()) {
                case "toString":
                    return method.getDeclaringClass().getSimpleName();
                case "equals":
                    return Proxy.isProxyClass(args[0].getClass()) && (Proxy.getInvocationHandler(args[0]) == this);
                case "hashCode":
                    return System.identityHashCode(this);
            }
            throw new UnsupportedOperationException(method.getName());
        }

        if (args == null) {
            args = NO_ARGS;
        }

        if ((args.length == 0) && "close".equals(method.getName())) {
            return null;
        }

        SwiftMethodHandler methodHandler = methods.get(method);

        try {
            if (methodHandler == null) {
                throw new TApplicationException(UNKNOWN_METHOD, "Unknown method: " + method);
            }

            ListenableFuture<Object> future = methodHandler.invoke(addressSelectionContext, headers, asList(args));

            if (methodHandler.isAsync()) {
                return unwrapUserException(future);
            }

            try {
                return future.get();
            }
            catch (ExecutionException e) {
                throw unwrapUserException(e.getCause());
            }
        }
        catch (Exception e) {
            // rethrow any exceptions declared to be thrown by the method
            for (Class<?> exceptionType : method.getExceptionTypes()) {
                if (exceptionType.isAssignableFrom(e.getClass())) {
                    throw e;
                }
            }

            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                throw new RuntimeTException("Thread interrupted", new TException(e));
            }

            if (e instanceof TApplicationException) {
                throw new RuntimeTApplicationException(e.getMessage(), (TApplicationException) e);
            }

            if (e instanceof TProtocolException) {
                throw new RuntimeTProtocolException(e.getMessage(), (TProtocolException) e);
            }

            if (e instanceof TTransportException) {
                throw new RuntimeTTransportException(e.getMessage(), (TTransportException) e);
            }

            if (e instanceof TException) {
                throw new RuntimeTException(e.getMessage(), (TException) e);
            }

            throw new RuntimeTException(e.getMessage(), new TException(e));
        }
    }

    private static ListenableFuture<Object> unwrapUserException(ListenableFuture<Object> future)
    {
        SettableFuture<Object> result = SettableFuture.create();
        Futures.addCallback(future, new FutureCallback<Object>()
        {
            @Override
            public void onSuccess(Object value)
            {
                result.set(value);
            }

            @Override
            public void onFailure(Throwable t)
            {
                result.setException(unwrapUserException(t));
            }
        });
        return result;
    }

    private static Throwable unwrapUserException(Throwable t)
    {
        // unwrap deserialized user exception
        return (t instanceof SwiftApplicationException) ? t.getCause() : t;
    }
}
