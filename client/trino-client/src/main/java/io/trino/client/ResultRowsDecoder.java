/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.client;

import io.trino.client.spooling.DataAttributes;
import io.trino.client.spooling.EncodedQueryData;
import io.trino.client.spooling.InlineSegment;
import io.trino.client.spooling.Segment;
import io.trino.client.spooling.SegmentLoader;
import io.trino.client.spooling.SpooledSegment;
import io.trino.client.spooling.encoding.QueryDataDecoders;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Iterators.transform;
import static io.trino.client.ResultRows.NULL_ROWS;
import static java.util.Objects.requireNonNull;

/**
 * Class responsible for decoding any QueryData type.
 */
public class ResultRowsDecoder
        implements AutoCloseable
{
    private final SegmentLoader loader;
    private QueryDataDecoder decoder;

    public ResultRowsDecoder()
    {
        this(new OkHttpSegmentLoader());
    }

    public ResultRowsDecoder(SegmentLoader loader)
    {
        this.loader = requireNonNull(loader, "loader is null");
    }

    private void setEncoding(List<Column> columns, String encoding)
    {
        if (decoder != null) {
            checkState(decoder.encoding().equals(encoding), "Decoder is configured for encoding %s but got %s", decoder.encoding(), encoding);
        }
        else {
            checkState(!columns.isEmpty(), "Columns must be set when decoding data");
            this.decoder = QueryDataDecoders.get(encoding)
                    // we don't use query-level attributes for now
                    .create(columns, DataAttributes.empty());
        }
    }

    public ResultRows toRows(QueryResults results)
    {
        if (results == null || results.getData() == null) {
            return NULL_ROWS;
        }

        return toRows(results.getColumns(), results.getData());
    }

    public ResultRows toRows(List<Column> columns, QueryData data)
    {
        if (data == null || data.isNull()) {
            return NULL_ROWS; // for backward compatibility instead of null
        }

        verify(columns != null && !columns.isEmpty(), "Columns must be set when decoding data");
        if (data instanceof TypedQueryData) {
            TypedQueryData rawData = (TypedQueryData) data;
            if (rawData.isNull()) {
                return NULL_ROWS; // for backward compatibility instead of null
            }
            // RawQueryData is always typed
            return () -> rawData.getIterable().iterator();
        }

        if (data instanceof JsonQueryData) {
            JsonQueryData jsonData = (JsonQueryData) data;
            if (jsonData.isNull()) {
                return NULL_ROWS;
            }
            return () -> {
                try {
                    return JsonIterators.forJsonParser(jsonData.getJsonParser(), columns);
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            };
        }

        if (data instanceof EncodedQueryData) {
            EncodedQueryData encodedData = (EncodedQueryData) data;
            setEncoding(columns, encodedData.getEncoding());
            return () -> concat(transform(encodedData.getSegments().iterator(), this::segmentToRows));
        }

        throw new UnsupportedOperationException("Unsupported data type: " + data.getClass().getName());
    }

    private Iterator<List<Object>> segmentToRows(Segment segment)
    {
        if (segment instanceof InlineSegment) {
            return ((InlineSegment) segment).toIterator(decoder);
        }

        if (segment instanceof SpooledSegment) {
            return ((SpooledSegment) segment).toIterator(loader, decoder);
        }

        throw new UnsupportedOperationException("Unsupported segment type: " + segment.getClass().getName());
    }

    public Optional<String> getEncoding()
    {
        return Optional.ofNullable(decoder)
                .map(QueryDataDecoder::encoding);
    }

    @Override
    public void close()
            throws Exception
    {
        loader.close();
    }
}
