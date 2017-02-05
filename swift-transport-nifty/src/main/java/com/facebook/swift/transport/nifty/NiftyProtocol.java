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

import com.facebook.nifty.core.TChannelBufferInputTransport;
import com.facebook.nifty.core.TChannelBufferOutputTransport;
import com.facebook.nifty.duplex.TDuplexProtocolFactory;
import com.facebook.nifty.duplex.TProtocolPair;
import com.facebook.swift.codec.ThriftCodec;
import com.facebook.swift.codec.internal.TProtocolReader;
import com.facebook.swift.codec.internal.TProtocolWriter;
import com.facebook.swift.codec.metadata.ThriftType;
import com.facebook.swift.transport.MethodMetadata;
import com.facebook.swift.transport.ParameterMetadata;
import com.facebook.swift.transport.SwiftApplicationException;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.jboss.netty.buffer.ChannelBuffer;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.List;

import static com.facebook.nifty.duplex.TTransportPair.fromSeparateTransports;
import static org.apache.thrift.TApplicationException.BAD_SEQUENCE_ID;
import static org.apache.thrift.TApplicationException.INVALID_MESSAGE_TYPE;
import static org.apache.thrift.TApplicationException.WRONG_METHOD_NAME;
import static org.apache.thrift.protocol.TMessageType.CALL;
import static org.apache.thrift.protocol.TMessageType.EXCEPTION;
import static org.apache.thrift.protocol.TMessageType.ONEWAY;
import static org.apache.thrift.protocol.TMessageType.REPLY;

@NotThreadSafe
public class NiftyProtocol
{
    private final TChannelBufferOutputTransport requestTransport;
    private final TChannelBufferInputTransport responseTransport;
    private final TProtocol requestProtocol;
    private final TProtocol responseProtocol;

    public NiftyProtocol(TDuplexProtocolFactory protocolFactory)
    {
        this.requestTransport = new TChannelBufferOutputTransport();
        this.responseTransport = new TChannelBufferInputTransport();

        TProtocolPair protocolPair = protocolFactory.getProtocolPair(fromSeparateTransports(responseTransport, requestTransport));
        this.requestProtocol = protocolPair.getOutputProtocol();
        this.responseProtocol = protocolPair.getInputProtocol();
    }

    public ChannelBuffer writeRequest(int sequenceId, MethodMetadata method, List<Object> parameters)
            throws Exception
    {
        requestTransport.resetOutputBuffer();
        writeRequest(requestProtocol, sequenceId, method, parameters);
        return requestTransport.getOutputBuffer().copy();
    }

    private static void writeRequest(TProtocol requestProtocol, int sequenceId, MethodMetadata method, List<Object> args)
            throws Exception
    {
        // Note that though setting message type to ONEWAY can be helpful when looking at packet
        // captures, some clients always send CALL and so servers are forced to rely on the "oneway"
        // attribute on thrift method in the interface definition, rather than checking the message
        // type.
        requestProtocol.writeMessageBegin(new TMessage(method.getName(), method.isOneway() ? ONEWAY : CALL, sequenceId));

        // write the parameters
        TProtocolWriter writer = new TProtocolWriter(requestProtocol);
        writer.writeStructBegin(method.getName() + "_args");
        for (int i = 0; i < args.size(); i++) {
            Object value = args.get(i);
            ParameterMetadata parameter = method.getParameters().get(i);
            writer.writeField(parameter.getName(), parameter.getId(), parameter.getCodec(), value);
        }
        writer.writeStructEnd();

        requestProtocol.writeMessageEnd();
        requestProtocol.getTransport().flush();
    }

    public Object readResponse(ChannelBuffer buffer, int sequenceId, MethodMetadata method)
            throws Exception
    {
        responseTransport.setInputBuffer(buffer);
        return readResponse(responseProtocol, sequenceId, method);
    }

    private static Object readResponse(TProtocol responseProtocol, int sequenceId, MethodMetadata method)
            throws Exception
    {
        // validate response header
        TMessage message = responseProtocol.readMessageBegin();
        if (message.type == EXCEPTION) {
            TApplicationException exception = TApplicationException.read(responseProtocol);
            responseProtocol.readMessageEnd();
            throw exception;
        }
        if (message.type != REPLY) {
            throw new TApplicationException(INVALID_MESSAGE_TYPE, "Received invalid message type " + message.type + " from server");
        }
        if (!message.name.equals(method.getName())) {
            throw new TApplicationException(WRONG_METHOD_NAME, "Wrong method name in reply: expected " + method.getName() + " but received " + message.name);
        }
        if (message.seqid != sequenceId) {
            throw new TApplicationException(BAD_SEQUENCE_ID, method.getName() + " failed: out of sequence response");
        }

        // read response struct
        TProtocolReader reader = new TProtocolReader(responseProtocol);
        reader.readStructBegin();

        Object results = null;
        Exception exception = null;
        while (reader.nextField()) {
            if (reader.getFieldId() == 0) {
                results = reader.readField(method.getResultCodec());
            }
            else {
                ThriftCodec<Object> exceptionCodec = method.getExceptionCodecs().get(reader.getFieldId());
                if (exceptionCodec != null) {
                    exception = (Exception) reader.readField(exceptionCodec);
                }
                else {
                    reader.skipFieldData();
                }
            }
        }
        reader.readStructEnd();
        responseProtocol.readMessageEnd();

        if (exception != null) {
            throw new SwiftApplicationException(exception);
        }

        if (method.getResultCodec().getType() == ThriftType.VOID) {
            return null;
        }

        if (results == null) {
            throw new TApplicationException(TApplicationException.MISSING_RESULT, method.getName() + " failed: unknown result");
        }
        return results;
    }
}
