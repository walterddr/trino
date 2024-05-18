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
package io.trino.plugin.lance.internal;

import com.google.common.base.Joiner;
import io.airlift.slice.Slice;
import io.trino.plugin.lance.LanceColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.commons.codec.binary.Hex;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class LanceUtils
{
    public static boolean isSupportedFilter(LanceColumnHandle columnHandle, Domain domain)
    {
        ValueSet valueSet = domain.getValues();
        boolean isNotNull = valueSet.isAll() && !domain.isNullAllowed();
        boolean isInOrNull = !valueSet.getRanges().getOrderedRanges().isEmpty() && domain.isNullAllowed();
        return isNotNull || domain.isOnlyNull() || isInOrNull;
    }

    public static Optional<String> toPredicate(LanceColumnHandle columnHandle, Domain domain)
    {
        String predicateArgument = quoteIdentifier(columnHandle.getColumnMetadata().getName());
        ValueSet valueSet = domain.getValues();
        if (valueSet.isNone()) {
            verify(!domain.isNullAllowed(), "NOT HANDLING NULL check");
            return Optional.of(format("(%s != %s)", predicateArgument, predicateArgument));
        }
        if (valueSet.isAll()) {
            verify(domain.isNullAllowed(), "NOT HANDLING NULL check");
            return Optional.empty();
        }
        verify(!domain.getValues().getRanges().getOrderedRanges().isEmpty() && !domain.isNullAllowed(), "NOT HANDLING NULL check");
        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();
        boolean invertPredicate = false;
        if (!valueSet.isDiscreteSet()) {
            ValueSet complement = domain.getValues().complement();
            if (complement.isDiscreteSet()) {
                invertPredicate = complement.isDiscreteSet();
                valueSet = complement;
            }
        }
        for (Range range : valueSet.getRanges().getOrderedRanges()) {
            checkState(!range.isAll()); // Already checked
            if (range.isSingleValue()) {
                singleValues.add(convertValue(range.getType(), range.getSingleValue()));
            }
            else {
                List<String> rangeConjuncts = new ArrayList<>();
                if (!range.isLowUnbounded()) {
                    rangeConjuncts.add(toConjunct(predicateArgument, range.isLowInclusive() ? ">=" : ">", convertValue(range.getType(), range.getLowBoundedValue())));
                }
                if (!range.isHighUnbounded()) {
                    rangeConjuncts.add(toConjunct(predicateArgument, range.isHighInclusive() ? "<=" : "<", convertValue(range.getType(), range.getHighBoundedValue())));
                }
                // If rangeConjuncts is null, then the range was ALL, which is not supported in pql
                checkState(!rangeConjuncts.isEmpty());
                disjuncts.add("(" + Joiner.on(" AND ").join(rangeConjuncts) + ")");
            }
        }
        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            String operator = invertPredicate ? "!=" : "=";
            disjuncts.add(toConjunct(predicateArgument, operator, getOnlyElement(singleValues)));
        }
        else if (singleValues.size() > 1) {
            String operator = invertPredicate ? "NOT IN" : "IN";
            disjuncts.add(inClauseValues(predicateArgument, operator, singleValues));
        }
        return Optional.of("(" + Joiner.on(" OR ").join(disjuncts) + ")");
    }

    private static Object convertValue(Type type, Object value)
    {
        if (type instanceof RealType) {
            return intBitsToFloat(toIntExact((Long) value));
        } else if (type instanceof VarcharType) {
            return ((Slice) value).toStringUtf8();
        } else if (type instanceof VarbinaryType) {
            return Hex.encodeHexString(((Slice) value).getBytes());
        } else {
            // Default value without conversion
            return value;
        }
    }

    private static String toConjunct(String columnName, String operator, Object value)
    {
        if (value instanceof Slice) {
            value = ((Slice) value).toStringUtf8();
        }
        return format("%s %s %s", columnName, operator, singleQuote(value));
    }

    private static String inClauseValues(String columnName, String operator, List<Object> singleValues)
    {
        return format("%s %s (%s)", columnName, operator, singleValues.stream()
                .map(LanceUtils::singleQuote)
                .collect(joining(", ")));
    }

    private static String singleQuote(Object value)
    {
        if (value instanceof String string) {
            return format("'%s'", string.replace("'", "''"));
        }
        return format("'%s'", value);
    }

    private static String quoteIdentifier(String identifier)
    {
        return format("\"%s\"", identifier.replaceAll("\"", "\"\""));
    }
}
