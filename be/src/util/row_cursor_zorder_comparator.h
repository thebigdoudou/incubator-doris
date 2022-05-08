// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef DORIS_BE_SRC_UTIL_ROW_CURSOR_ZORDER_COMPARATOR_H
#define DORIS_BE_SRC_UTIL_ROW_CURSOR_ZORDER_COMPARATOR_H

#include "exec/sort_exec_exprs.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "olap/row.h"
#include "olap/row_cursor.h"
#include "olap/schema.h"
#include "runtime/descriptors.h"
#include "runtime/raw_value.h"
#include "runtime/tuple.h"
#include "runtime/tuple_row.h"

namespace doris {
class RowComparator {
public:
    RowComparator() = default;
    RowComparator(Schema* schema){};
    virtual ~RowComparator() = default;
    virtual int operator()(const char* left, const char* right) const{
        return -1;
    }
};

class RowCurosrZOrderComparator : public RowComparator {
public:
    RowCurosrZOrderComparator()
        : _schema(nullptr),_sort_col_num(0){}

    RowCurosrZOrderComparator(int sort_col_num)
        : _schema(nullptr),_sort_col_num(sort_col_num){}

    RowCurosrZOrderComparator(Schema* schema, int sort_col_num)
        : _schema(schema),_sort_col_num(sort_col_num){
        max_col_size(_schema);
    }

    virtual ~RowCurosrZOrderComparator() {}

    virtual int operator()(const char* lhs, const char* rhs) const{
        int result = compare(lhs, rhs);
        return result;
    }

    void max_col_size(const Schema* schema){
        _max_col_size = get_type_byte_size(schema->column(0)->type());
        for (size_t i = 1; i < _sort_col_num; ++i) {
            if (_max_col_size <  get_type_byte_size(schema->column(i)->type())) {
                _max_col_size =  get_type_byte_size(schema->column(i)->type());
            }
        }
    }

    int compare(const char* lhs, const char* rhs) const{
        ContiguousRow lhs_row(_schema, lhs);
        ContiguousRow rhs_row(_schema, rhs);
        if (_max_col_size <= 4) {
            return compare_based_on_size<uint32_t, ContiguousRow>(lhs_row, rhs_row);
        } else if (_max_col_size <= 8) {
            return compare_based_on_size<uint64_t, ContiguousRow>(lhs_row, rhs_row);
        } else {
            return compare_based_on_size<uint128_t, ContiguousRow>(lhs_row, rhs_row);
        }
    }

    int compare_row(const RowCursor& lhs, const RowCursor& rhs){
        max_col_size(lhs.schema());
        if (_max_col_size <= 4) {
            return compare_based_on_size<uint32_t, const RowCursor>(lhs, rhs);
        } else if (_max_col_size <= 8) {
            return compare_based_on_size<uint64_t, const RowCursor>(lhs, rhs);
        } else {
            return compare_based_on_size<uint128_t, const RowCursor>(lhs, rhs);
        }
    }

    template <typename U, typename LhsRowType>
    int compare_based_on_size(LhsRowType& lhs, LhsRowType& rhs) const{
        // for example, x is 0100, y is 0101, it's necessary to ensure that x < (x ^ y) 
        auto less_msb = [](U x, U y) { return x < y && x < (x ^ y); };
        FieldType type = lhs.schema()->column(0)->type();
        U msd_lhs = get_shared_representation<U>(lhs.cell(0).is_null() ? nullptr : lhs.cell(0).cell_ptr(),
                                                 type);
        U msd_rhs = get_shared_representation<U>(rhs.cell(0).is_null() ? nullptr : rhs.cell(0).cell_ptr(),
                                                 type);
        for (int i = 1; i < _sort_col_num; ++i) {
            type = lhs.schema()->column(i)->type();
            const void *lhs_v = lhs.cell(i).is_null() ? nullptr : lhs.cell(i).cell_ptr();
            const void *rhs_v = rhs.cell(i).is_null() ? nullptr : rhs.cell(i).cell_ptr();
            U lhsi = get_shared_representation<U>(lhs_v, type);
            U rhsi = get_shared_representation<U>(rhs_v, type);
            if (less_msb(msd_lhs ^ msd_rhs, lhsi ^ rhsi)) {
                msd_lhs = lhsi;
                msd_rhs = rhsi;
            }
        }
        return msd_lhs < msd_rhs ? -1 : (msd_lhs > msd_rhs ? 1 : 0);
    }

    template <typename U>
    U get_shared_representation(const void* val, FieldType type) const{
        if (val == NULL) return 0;
        constexpr U mask = (U) 1 << (sizeof(U) * 8 - 1);
        switch (type) {
            case FieldType::OLAP_FIELD_TYPE_NONE:
                return 0;
            case FieldType::OLAP_FIELD_TYPE_BOOL:
                return static_cast<U>(*reinterpret_cast<const bool *>(val)) << (sizeof(U) * 8 - 1);
            case FieldType::OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
                return get_shared_int_representation<U, int8_t>(
                        *reinterpret_cast<const int8_t *>(val), static_cast<U>(0));
            case FieldType::OLAP_FIELD_TYPE_TINYINT:
                return get_shared_int_representation<U, int8_t>(
                        *reinterpret_cast<const int8_t *>(val), mask);
            case FieldType::OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
                return get_shared_int_representation<U, int16_t>(
                        *reinterpret_cast<const int16_t *>(val), static_cast<U>(0));
            case FieldType::OLAP_FIELD_TYPE_SMALLINT:
                return get_shared_int_representation<U, int16_t>(
                        *reinterpret_cast<const int16_t *>(val), mask);
            case FieldType::OLAP_FIELD_TYPE_INT:
                return get_shared_int_representation<U, int32_t>(
                        *reinterpret_cast<const int32_t *>(val), mask);
            case FieldType::OLAP_FIELD_TYPE_DATETIME:
            case FieldType::OLAP_FIELD_TYPE_DATE:
            case FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
                return get_shared_int_representation<U, int64_t>(
                        *reinterpret_cast<const int64_t *>(val), static_cast<U>(0));
            case FieldType::OLAP_FIELD_TYPE_BIGINT:
                return get_shared_int_representation<U, int64_t>(
                        *reinterpret_cast<const int64_t *>(val), mask);
            case FieldType::OLAP_FIELD_TYPE_LARGEINT:
                return static_cast<U>(*reinterpret_cast<const int128_t *>(val)) ^ mask;
            case FieldType::OLAP_FIELD_TYPE_FLOAT:
                return get_shared_float_representation<U, float>(val, mask);
            case FieldType::OLAP_FIELD_TYPE_DISCRETE_DOUBLE:
            case FieldType::OLAP_FIELD_TYPE_DOUBLE:
                return get_shared_float_representation<U, double>(val, mask);
            case FieldType::OLAP_FIELD_TYPE_CHAR:
            case FieldType::OLAP_FIELD_TYPE_VARCHAR:{
                const StringValue *string_value = reinterpret_cast<const StringValue *>(val);
                return get_shared_string_representation<U>(string_value->ptr, string_value->len);
            }
            case FieldType::OLAP_FIELD_TYPE_DECIMAL: {
                decimal12_t decimal_val = *reinterpret_cast<const decimal12_t*>(val);
                int128_t value = decimal_val.integer*DecimalV2Value::ONE_BILLION + decimal_val.fraction;
                return static_cast<U>(value) ^ mask;
            }
            default:
                return 0;
        }
    }

    template <typename U, typename T>
    U get_shared_int_representation(const T val, U mask) const{
        uint64_t shift_size = static_cast<uint64_t>(
                std::max(static_cast<int64_t>((sizeof(U) - sizeof(T)) * 8), (int64_t) 0));
        return (static_cast<U>(val) << shift_size) ^ mask;
    }

    template <typename U, typename T>
    U get_shared_float_representation(const void* val, U mask) const{
        int64_t tmp;
        T floating_value = *reinterpret_cast<const T *>(val);
        memcpy(&tmp, &floating_value, sizeof(T));
        if (UNLIKELY(std::isnan(floating_value))) return 0;
        int s = (int)((sizeof(U) - sizeof(T)) * 8);
        if (floating_value < 0.0) {
            // Flipping all bits for negative values.
            return static_cast<U>(~tmp) << std::max(s, 0);
        } else {
            // Flipping only first bit.
            return (static_cast<U>(tmp) << std::max(s, 0)) ^ mask;
        }
    }

    template <typename U>
    U get_shared_string_representation(const char* char_ptr, int length) const{
        int len = length < sizeof(U) ? length : sizeof(U);
        if (len == 0) return 0;
        U dst = 0;
        ByteSwapScalar(&dst, char_ptr, len);
        return dst << ((sizeof(U) - len) * 8);
    }

    int get_type_byte_size(FieldType type) const{
        switch (type) {
            case FieldType::OLAP_FIELD_TYPE_OBJECT:
            case FieldType::OLAP_FIELD_TYPE_HLL:
            case FieldType::OLAP_FIELD_TYPE_STRUCT:
            case FieldType::OLAP_FIELD_TYPE_ARRAY:
            case FieldType::OLAP_FIELD_TYPE_MAP:
            case FieldType::OLAP_FIELD_TYPE_CHAR:
            case FieldType::OLAP_FIELD_TYPE_VARCHAR:
                return 0;
            case FieldType::OLAP_FIELD_TYPE_NONE:
            case FieldType::OLAP_FIELD_TYPE_BOOL:
            case FieldType::OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
            case FieldType::OLAP_FIELD_TYPE_TINYINT:
                return 1;
            case FieldType::OLAP_FIELD_TYPE_SMALLINT:
            case FieldType::OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
                return 2;
            case FieldType::OLAP_FIELD_TYPE_FLOAT:
            case FieldType::OLAP_FIELD_TYPE_INT:
            case FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT:
                return 4;
            case FieldType::OLAP_FIELD_TYPE_DISCRETE_DOUBLE:
            case FieldType::OLAP_FIELD_TYPE_DOUBLE:
            case FieldType::OLAP_FIELD_TYPE_BIGINT:
            case FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
                return 8;
            case FieldType::OLAP_FIELD_TYPE_DECIMAL:
            case FieldType::OLAP_FIELD_TYPE_LARGEINT:
            case FieldType::OLAP_FIELD_TYPE_DATETIME:
            case FieldType::OLAP_FIELD_TYPE_DATE:
                return 16;
            case FieldType::OLAP_FIELD_TYPE_UNKNOWN:
                DCHECK(false);
                break;
            default:
                DCHECK(false);
        }
        return -1;
    }
    
    void ByteSwapScalar(void *dest, const void *source, int len) const{
        uint8_t *dst = reinterpret_cast<uint8_t *>(dest);
        const uint8_t *src = reinterpret_cast<const uint8_t *>(source);
        switch (len) {
            case 1:
                *reinterpret_cast<uint8_t *>(dst) = *reinterpret_cast<const uint8_t *>(src);
                return;
            case 2:
                *reinterpret_cast<uint16_t *>(dst) =
                        BitUtil::byte_swap(*reinterpret_cast<const uint16_t *>(src));
                return;
            case 3:
                *reinterpret_cast<uint16_t *>(dst + 1) =
                        BitUtil::byte_swap(*reinterpret_cast<const uint16_t *>(src));
                *reinterpret_cast<uint8_t *>(dst) = *reinterpret_cast<const uint8_t *>(src + 2);
                return;
            case 4:
                *reinterpret_cast<uint32_t *>(dst) =
                        BitUtil::byte_swap(*reinterpret_cast<const uint32_t *>(src));
                return;
            case 5:
                *reinterpret_cast<uint32_t *>(dst + 1) =
                        BitUtil::byte_swap(*reinterpret_cast<const uint32_t *>(src));
                *reinterpret_cast<uint8_t *>(dst) = *reinterpret_cast<const uint8_t *>(src + 4);
                return;
            case 6:
                *reinterpret_cast<uint32_t *>(dst + 2) =
                        BitUtil::byte_swap(*reinterpret_cast<const uint32_t *>(src));
                *reinterpret_cast<uint16_t *>(dst) =
                        BitUtil::byte_swap(*reinterpret_cast<const uint16_t *>(src + 4));
                return;
            case 7:
                *reinterpret_cast<uint32_t *>(dst + 3) =
                        BitUtil::byte_swap(*reinterpret_cast<const uint32_t *>(src));
                *reinterpret_cast<uint16_t *>(dst + 1) =
                        BitUtil::byte_swap(*reinterpret_cast<const uint16_t *>(src + 4));
                *reinterpret_cast<uint8_t *>(dst) = *reinterpret_cast<const uint8_t *>(src + 6);
                return;
            case 8:
                *reinterpret_cast<uint64_t *>(dst) =
                        BitUtil::byte_swap(*reinterpret_cast<const uint64_t *>(src));
                return;
            case 9:
                *reinterpret_cast<uint64_t *>(dst + 1) =
                        BitUtil::byte_swap(*reinterpret_cast<const uint64_t *>(src));
                *reinterpret_cast<uint8_t *>(dst) = *reinterpret_cast<const uint8_t *>(src + 8);
                return;
            case 10:
                *reinterpret_cast<uint64_t *>(dst + 2) =
                        BitUtil::byte_swap(*reinterpret_cast<const uint64_t *>(src));
                *reinterpret_cast<uint16_t *>(dst) =
                        BitUtil::byte_swap(*reinterpret_cast<const uint16_t *>(src + 8));
                return;
            case 11:
                *reinterpret_cast<uint64_t *>(dst + 3) =
                        BitUtil::byte_swap(*reinterpret_cast<const uint64_t *>(src));
                *reinterpret_cast<uint16_t *>(dst + 1) =
                        BitUtil::byte_swap(*reinterpret_cast<const uint16_t *>(src + 8));
                *reinterpret_cast<uint8_t *>(dst) = *reinterpret_cast<const uint8_t *>(src + 10);
                return;
            case 12:
                *reinterpret_cast<uint64_t *>(dst + 4) =
                        BitUtil::byte_swap(*reinterpret_cast<const uint64_t *>(src));
                *reinterpret_cast<uint32_t *>(dst) =
                        BitUtil::byte_swap(*reinterpret_cast<const uint32_t *>(src + 8));
                return;
            case 13:
                *reinterpret_cast<uint64_t *>(dst + 5) =
                        BitUtil::byte_swap(*reinterpret_cast<const uint64_t *>(src));
                *reinterpret_cast<uint32_t *>(dst + 1) =
                        BitUtil::byte_swap(*reinterpret_cast<const uint32_t *>(src + 8));
                *reinterpret_cast<uint8_t *>(dst) = *reinterpret_cast<const uint8_t *>(src + 12);
                return;
            case 14:
                *reinterpret_cast<uint64_t *>(dst + 6) =
                        BitUtil::byte_swap(*reinterpret_cast<const uint64_t *>(src));
                *reinterpret_cast<uint32_t *>(dst + 2) =
                        BitUtil::byte_swap(*reinterpret_cast<const uint32_t *>(src + 8));
                *reinterpret_cast<uint16_t *>(dst) =
                        BitUtil::byte_swap(*reinterpret_cast<const uint16_t *>(src + 12));
                return;
            case 15:
                *reinterpret_cast<uint64_t *>(dst + 7) =
                        BitUtil::byte_swap(*reinterpret_cast<const uint64_t *>(src));
                *reinterpret_cast<uint32_t *>(dst + 3) =
                        BitUtil::byte_swap(*reinterpret_cast<const uint32_t *>(src + 8));
                *reinterpret_cast<uint16_t *>(dst + 1) =
                        BitUtil::byte_swap(*reinterpret_cast<const uint16_t *>(src + 12));
                *reinterpret_cast<uint8_t *>(dst) = *reinterpret_cast<const uint8_t *>(src + 14);
                return;
            case 16:
                *reinterpret_cast<uint64_t *>(dst + 8) =
                        BitUtil::byte_swap(*reinterpret_cast<const uint64_t *>(src));
                *reinterpret_cast<uint64_t *>(dst) =
                        BitUtil::byte_swap(*reinterpret_cast<const uint64_t *>(src + 8));
                return;
            default:
                // Revert to slow loop-based swap.
                uint8_t *d = reinterpret_cast<uint8_t *>(dest);
                const uint8_t *s = reinterpret_cast<const uint8_t *>(source);
                for (int i = 0; i < len; ++i) d[i] = s[len - i - 1];
                return;
        }
    }

private:
    int _max_col_size = 0;
    const Schema* _schema;
    int _sort_col_num = 0;
};
} // namespace doris

#endif  //DORIS_BE_SRC_UTIL_ROW_CURSOR_ZORDER_COMPARATOR_H
