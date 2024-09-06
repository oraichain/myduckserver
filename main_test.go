// Copyright 2024 ApeCloud, Inc.

// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
	_ "github.com/dolthub/go-mysql-server/sql/variables"
)

type indexBehaviorTestParams struct {
	name              string
	driverInitializer IndexDriverInitializer
	nativeIndexes     bool
}

var numPartitionsVals = []int{
	1,
	testNumPartitions,
}
var indexBehaviors = []*indexBehaviorTestParams{
	{"none", nil, false},
	{"mergableIndexes", mergableIndexDriver, false},
	{"nativeIndexes", nil, true},
	{"nativeAndMergable", mergableIndexDriver, true},
}
var parallelVals = []int{
	1,
	2,
}

func TestDebugHarness(t *testing.T) {
	t.Skip("only used for debugging")

	harness := NewDuckHarness("debug", 1, 1, true, nil)

	setupData := []setup.SetupScript{{
		`create database if not exists mydb`,
		`use mydb`,
		"create table mytable (i bigint primary key, s CHAR(20) comment 'column s' NOT NULL)",
	}}

	harness.Setup(setupData)
	engine, err := harness.NewEngine(t)
	require.NoError(t, err)

	engine.EngineAnalyzer().Debug = true
	engine.EngineAnalyzer().Verbose = true

	ctx := enginetest.NewContext(harness)
	_, iter, _, err := engine.Query(ctx, "create index mytable_i_s on mytable (i,s)")
	require.NoError(t, err)
	defer iter.Close(ctx)

	for {
		row, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		fmt.Println(row)
	}

}

var (
	notApplicableQueries = []string{
		"SELECT_*_FROM_mytable_t0_INNER_JOIN_mytable_t1_ON_(t1.i_IN_(((true)%(''))));",
		"SELECT_count(*)_from_mytable_WHERE_(i_IN_(-''));",
		"select_sum('abc')_from_mytable",
	}

	waitForFixQueries = []string{
		"select_*_from_(select_i,_i2_from_niltable)_a(x,y)_union_select_*_from_(select_1,_NULL)_b(x,y)_union_select_*_from_(select_i,_i2_from_niltable)_c(x,y)",
		"SELECT_SUM(i),_i_FROM_mytable_GROUP_BY_i_ORDER_BY_1+SUM(i)_ASC",
		"SELECT_SUM(i)_as_sum,_i_FROM_mytable_GROUP_BY_i_ORDER_BY_1+SUM(i)_ASC",
		"select_sum(10)_from_mytable",
		"select_(select_count(*)_from_xy),_(select_count(*)_from_uv)",
		"_Select_x_from_(select_*_from_xy)_sq1_union_all_select_u_from_(select_*_from_uv)_sq2_limit_1_offset_1;",
		"_Select_*_from_(___With_recursive_cte(s)_as_(select_1_union_select_x_from_xy_join_cte_on_x_=_s)___Select_*_from_cte___Union___Select_x_from_xy_where_x_in_(select_*_from_cte)__)_dt;",
		"select_count(*)_from_typestable_where_e1_in_('hi',_'bye');",
		"select_count(*)_from_typestable_where_e1_in_('',_'bye');",
		"select_count(*)_from_typestable_where_s1_in_('hi',_'bye');",
		"select_count(*)_from_typestable_where_s1_in_('',_'bye');",
		"select_count(*)_from_mytable_where_s_in_(1,_'first_row');",
		"SELECT_count(*),_i,_concat(i,_i),_123,_'abc',_concat('abc',_'def')_FROM_emptytable;",
		"SELECT_pk_FROM_one_pk_WHERE_(pk,_123)_IN_(SELECT_count(*)_AS_u,_123_AS_v_FROM_emptytable);",
		"select_pk_from_one_pk_where_(pk,_123)_in_(select_count(*)_as_u,_123_as_v_from_mytable_where_false);",
		"select_pk_from_one_pk_where_(pk,_123)_not_in_(select_count(*)_as_u,_123_as_v_from_emptytable);",
		"select_pk_from_one_pk_where_(pk,_123)_not_in_(select_count(*)_as_u,_123_as_v_from_mytable_where_false);",
		"select_count(*),_(select_i_from_mytable_where_i_=_1_group_by_i);",
		"select_pk_div_2,_sum(c3)_from_one_pk_group_by_1_order_by_1",
		"select_pk_div_2,_sum(c3)_as_sum_from_one_pk_group_by_1_order_by_1",
		"select_pk_div_2,_sum(c3)_+_sum(c3)_as_sum_from_one_pk_group_by_1_order_by_1",
		"select_pk_div_2,_sum(c3)_+_min(c3)_as_sum_and_min_from_one_pk_group_by_1_order_by_1",
		"select_pk_div_2,_sum(`c3`)_+____min(_c3_)_from_one_pk_group_by_1_order_by_1",
		"select_pk1,_sum(c1)_from_two_pk_group_by_pk1_order_by_pk1;",
		"select_pk1,_sum(c1)_from_two_pk_where_pk1_=_0",
		"select_floor(i),_s_from_mytable_mt_order_by_floor(i)_desc",
		"select_floor(i),_avg(char_length(s))_from_mytable_mt_group_by_1_order_by_floor(i)_desc",
		"select_format(i,_3)_from_mytable;",
		"select_format(i,_3,_'da_dk')_from_mytable;",
		"SELECT_JSON_KEYS(c3)_FROM_jsontable",
		"SELECT_JSON_OVERLAPS(c3,_'{\"a\":_2,_\"d\":_2}')_FROM_jsontable",
		"SELECT_JSON_MERGE(c3,_'{\"a\":_1}')_FROM_jsontable",
		"SELECT_JSON_MERGE_PRESERVE(c3,_'{\"a\":_1}')_FROM_jsontable",
		"select_json_pretty(c3)_from_jsontable",
		"SELECT_i,_sum(i)_FROM_mytable_group_by_1_having_avg(i)_>_1_order_by_1",
		"SELECT_a.column_0,_mt.s_from_(values_row(1,\"1\"),_row(2,\"2\"),_row(4,\"4\"))_a____left_join_mytable_mt_on_column_0_=_mt.i____order_by_1",
		"WITH_mt_(s,i)_as_(select_char_length(s),_sum(i)_FROM_mytable_group_by_1)_SELECT_s,i_FROM_mt_order_by_1",
		"select_i+0.0/(lag(i)_over_(order_by_s))_from_mytable_order_by_1;",
		"select_f64/f32,_f32/(lag(i)_over_(order_by_f64))_from_floattable_order_by_1,2;",
		"SELECT_s2,_i2,_i____FROM_(SELECT_*_FROM_mytable)_mytable____RIGHT_JOIN_____((SELECT_i2,_s2_FROM_othertable_ORDER_BY_i2_ASC)______UNION_ALL______SELECT_CAST(4_AS_SIGNED)_AS_i2,_\"not_found\"_AS_s2_FROM_DUAL)_othertable____ON_i2_=_i",
		"WITH_mytable_as_(select_*_FROM_mytable)_SELECT_s,i_FROM_mytable;",
		"WITH_mytable_as_(select_*_FROM_mytable_where_i_>_2)_SELECT_*_FROM_mytable;",
		"WITH_mytable_as_(select_*_FROM_mytable_where_i_>_2)_SELECT_*_FROM_mytable_union_SELECT_*_from_mytable;",
		"____WITH_RECURSIVE_included_parts(sub_part,_part,_quantity)_AS_(_____SELECT_sub_part,_part,_quantity_FROM_parts_WHERE_part_=_'pie'______UNION_ALL_____SELECT_p.sub_part,_p.part,_p.quantity_____FROM_included_parts_AS_pr,_parts_AS_p_____WHERE_p.part_=_pr.sub_part____)____SELECT_sub_part,_sum(quantity)_as_total_quantity____FROM_included_parts____GROUP_BY_sub_part",
		"____WITH_RECURSIVE_included_parts(sub_part,_part,_quantity)_AS_(_____SELECT_sub_part,_part,_quantity_FROM_parts_WHERE_lower(part)_=_'pie'______UNION_ALL_____SELECT_p.sub_part,_p.part,_p.quantity_____FROM_included_parts_AS_pr,_parts_AS_p_____WHERE_p.part_=_pr.sub_part____)____SELECT_sub_part,_sum(quantity)_as_total_quantity____FROM_included_parts____GROUP_BY_sub_part",
		"____WITH_RECURSIVE_included_parts(sub_part,_part,_quantity)_AS_(_____SELECT_sub_part,_part,_quantity_FROM_parts_WHERE_part_=_(select_part_from_parts_where_part_=_'pie'_and_sub_part_=_'crust')______UNION_ALL_____SELECT_p.sub_part,_p.part,_p.quantity_____FROM_included_parts_AS_pr,_parts_AS_p_____WHERE_p.part_=_pr.sub_part____)____SELECT_sub_part,_sum(quantity)_as_total_quantity____FROM_included_parts____GROUP_BY_sub_part",
		"SELECT_i,_1_AS_foo,_2_AS_bar_FROM_MyTable_HAVING_bar_=_2_ORDER_BY_foo,_i;",
		"SELECT_i,_1_AS_foo,_2_AS_bar_FROM_MyTable_HAVING_bar_=_1_ORDER_BY_foo,_i;",
		"SELECT_reservedWordsTable.AND,_reservedWordsTABLE.Or,_reservedwordstable.SEleCT_FROM_reservedWordsTable;",
		"SELECT_NULL_IN_(SELECT_i_FROM_emptytable)",
		"SELECT_NULL_NOT_IN_(SELECT_i_FROM_emptytable)",
		"SELECT_NULL_IN_(SELECT_i_FROM_mytable)",
		"SELECT_NULL_NOT_IN_(SELECT_i_FROM_mytable)",
		"SELECT_NULL_IN_(SELECT_i2_FROM_niltable)",
		"SELECT_NULL_NOT_IN_(SELECT_i2_FROM_niltable)",
		"SELECT_2_IN_(SELECT_i2_FROM_niltable)",
		"SELECT_2_NOT_IN_(SELECT_i2_FROM_niltable)",
		"SELECT_100_IN_(SELECT_i2_FROM_niltable)",
		"SELECT_100_NOT_IN_(SELECT_i2_FROM_niltable)",
		"SELECT_*_from_mytable_where_(i_=_1_|_false)_IN_(true)",
		"SELECT_*_from_mytable_where_(i_=_1_&_false)_IN_(true)",
		"SELECT_i_FROM_mytable_WHERE_'hello';",
		"SELECT_i_FROM_mytable_WHERE_NOT_'hello';",
		"select_i_from_datetime_table_where_date_col_=_'2019-12-31T00:00:01'",
		"select_i_from_datetime_table_where_datetime_col_=_datetime('2020-01-01T12:00:00')",
		"select_i_from_datetime_table_where_datetime_col_>=_'2020-01-01_00:00'_order_by_1",
		"select_i_from_datetime_table_where_datetime_col_>_datetime('2020-01-01T12:00:00')_order_by_1",
		"select_i_from_datetime_table_where_timestamp_col_=_datetime('2020-01-02T12:00:00')",
		"select_i_from_datetime_table_where_timestamp_col_>_datetime('2020-01-02T12:00:00')_order_by_1",
		"SELECT_SUBSTRING_INDEX(mytable.s,_\"d\",_1)_AS_s_FROM_mytable_INNER_JOIN_othertable_ON_(SUBSTRING_INDEX(mytable.s,_\"d\",_1)_=_SUBSTRING_INDEX(othertable.s2,_\"d\",_1))_GROUP_BY_1_HAVING_s_=_'secon';",
		"SELECT_SUBSTRING_INDEX(mytable.s,_\"d\",_1)_AS_s_FROM_mytable_INNER_JOIN_othertable_ON_(SUBSTRING_INDEX(mytable.s,_\"d\",_1)_=_SUBSTRING_INDEX(othertable.s2,_\"d\",_1))_GROUP_BY_s_HAVING_s_=_'secon';",
		"SELECT_SUBSTRING_INDEX(mytable.s,_\"d\",_1)_AS_ss_FROM_mytable_INNER_JOIN_othertable_ON_(SUBSTRING_INDEX(mytable.s,_\"d\",_1)_=_SUBSTRING_INDEX(othertable.s2,_\"d\",_1))_GROUP_BY_s_HAVING_s_=_'secon';",
		"SELECT_SUBSTRING_INDEX(mytable.s,_\"d\",_1)_AS_ss_FROM_mytable_INNER_JOIN_othertable_ON_(SUBSTRING_INDEX(mytable.s,_\"d\",_1)_=_SUBSTRING_INDEX(othertable.s2,_\"d\",_1))_GROUP_BY_ss_HAVING_ss_=_'secon';",
		"SELECT_TRIM(mytable.s_from_\"first_row\")_AS_s_FROM_mytable",
		"SELECT_HOUR('2007-12-11_20:21:22')_FROM_mytable",
		"SELECT_MINUTE('2007-12-11_20:21:22')_FROM_mytable",
		"SELECT_SECOND('2007-12-11_20:21:22')_FROM_mytable",
		"SELECT_SECOND('2007-12-11T20:21:22Z')_FROM_mytable",
		"SELECT_DAYOFYEAR('20071211')_FROM_mytable",
		"SELECT_DISTINCT_CAST(i_AS_DECIMAL)_from_mytable;",
		"SELECT_SUM(_DISTINCT_CAST(i_AS_DECIMAL))_from_mytable;",
		"SELECT_id_FROM_typestable_WHERE_ti_>_'2019-12-31'",
		"SELECT_id_FROM_typestable_WHERE_da_=_'2019-12-31'",
		"SELECT_id_FROM_typestable_WHERE_ti_<_'2019-12-31'",
		"SELECT_id_FROM_typestable_WHERE_da_<_'2019-12-31'",
		"SELECT_id_FROM_typestable_WHERE_ti_>_date_add('2019-12-30',_INTERVAL_1_day)",
		"SELECT_id_FROM_typestable_WHERE_da_>_date_add('2019-12-30',_INTERVAL_1_DAY)",
		"SELECT_id_FROM_typestable_WHERE_da_>=_date_add('2019-12-30',_INTERVAL_1_DAY)",
		"SELECT_id_FROM_typestable_WHERE_ti_<_date_add('2019-12-30',_INTERVAL_1_DAY)",
		"SELECT_id_FROM_typestable_WHERE_da_<_date_add('2019-12-30',_INTERVAL_1_DAY)",
		"SELECT_id_FROM_typestable_WHERE_da_<_adddate('2020-01-01',_INTERVAL_1_DAY)",
		"SELECT_id_FROM_typestable_WHERE_da_<_adddate('2020-01-01',_1)",
		"SELECT_id_FROM_typestable_WHERE_ti_>_date_sub('2020-01-01',_INTERVAL_1_DAY)",
		"SELECT_id_FROM_typestable_WHERE_da_>_date_sub('2020-01-01',_INTERVAL_1_DAY)",
		"SELECT_id_FROM_typestable_WHERE_da_>=_date_sub('2020-01-01',_INTERVAL_1_DAY)",
		"SELECT_id_FROM_typestable_WHERE_ti_<_date_sub('2020-01-01',_INTERVAL_1_DAY)",
		"SELECT_id_FROM_typestable_WHERE_da_<_date_sub('2020-01-01',_INTERVAL_1_DAY)",
		"SELECT_id_FROM_typestable_WHERE_da_>=_subdate('2020-01-01',_INTERVAL_1_DAY)",
		"SELECT_id_FROM_typestable_WHERE_da_>=_subdate('2020-01-01',_1)",
		"SELECT_adddate(da,_i32)_from_typestable;",
		"SELECT_adddate(da,_concat(u32))_from_typestable;",
		"SELECT_adddate(da,_f32/10)_from_typestable;",
		"SELECT_subdate(da,_i32)_from_typestable;",
		"SELECT_subdate(da,_concat(u32))_from_typestable;",
		"SELECT_subdate(da,_f32/10)_from_typestable;",
		"SELECT_i,v_from_stringandtable_WHERE_v",
		"SELECT_i,v_from_stringandtable_WHERE_v_AND_v",
		"SELECT_i,v_from_stringandtable_WHERE_v_OR_v",
		"SELECT_i,v_from_stringandtable_WHERE_NOT_v",
		"SELECT_i,v_from_stringandtable_WHERE_NOT_v_AND_NOT_v",
		"SELECT_i,v_from_stringandtable_WHERE_NOT_v_OR_NOT_v",
		"SELECT_i,v_from_stringandtable_WHERE_v_OR_NOT_v",
		"SELECT_i,v_from_stringandtable_WHERE_v_XOR_v",
		"SELECT_i,v_from_stringandtable_WHERE_NOT_v_XOR_NOT_v",
		"SELECT_i,v_from_stringandtable_WHERE_v_XOR_NOT_v",
		"select_pk,_________row_number()_over_(order_by_pk_desc),_________sum(v1)_over_(partition_by_v2_order_by_pk),_________percent_rank()_over(partition_by_v2_order_by_pk)_____from_one_pk_three_idx_order_by_pk",
		"select_pk,____________________percent_rank()_over(partition_by_v2_order_by_pk),____________________dense_rank()_over(partition_by_v2_order_by_pk),____________________rank()_over(partition_by_v2_order_by_pk)_____from_one_pk_three_idx_order_by_pk",
		"SELECT_CAST(-3_AS_UNSIGNED)_FROM_mytable",
		"SELECT_CONVERT(-3,_UNSIGNED)_FROM_mytable",
		"SELECT_s_>_2_FROM_tabletest",
		"SELECT_*_FROM_tabletest_WHERE_s_>_0",
		"SELECT_*_FROM_tabletest_WHERE_s_=_0",
		"SELECT_i_AS_foo_FROM_mytable_HAVING_foo_NOT_IN_(1,_2,_5)",
		"SELECT_SUM(i)_FROM_mytable",
		"SELECT_RAND(i)_from_mytable_order_by_i",
		"SELECT_MOD(i,_2)_from_mytable_order_by_i_limit_1",
		"SELECT_TAN(i)_from_mytable_order_by_i_limit_1",
		"SELECT_ASIN(i)_from_mytable_order_by_i_limit_1",
		"SELECT_ACOS(i)_from_mytable_order_by_i_limit_1",
		"SELECT_COT(i)_from_mytable_order_by_i_limit_1",
		"SELECT_CRC32(i)_from_mytable_order_by_i_limit_1",
		"SELECT_ASCII(s)_from_mytable_order_by_i_limit_1",
		"SELECT_UNHEX(s)_from_mytable_order_by_i_limit_1",
		"SELECT_BIT_LENGTH(i)_from_mytable_order_by_i_limit_1",
		"select_date_format(datetime_col,_'%D')_from_datetime_table_order_by_1",
		"select_time_format(time_col,_'%h%p')_from_datetime_table_order_by_1",
		"select_from_unixtime(i)_from_mytable_order_by_1",
		"SELECT_SUM(i)_+_1,_i_FROM_mytable_GROUP_BY_i_ORDER_BY_i",
		"SELECT_SUM(i)_as_sum,_i_FROM_mytable_GROUP_BY_i_ORDER_BY_sum_ASC",
		"SELECT_i,_SUM(i)_FROM_mytable_GROUP_BY_i_ORDER_BY_sum(i)_DESC",
		"SELECT_i,_SUM(i)_as_b_FROM_mytable_GROUP_BY_i_ORDER_BY_b_DESC",
		"SELECT_i,_SUM(i)_as_`sum(i)`_FROM_mytable_GROUP_BY_i_ORDER_BY_sum(i)_DESC",
		"describe_myview",
		"SELECT_CASE_WHEN_i_>_2_THEN_i_WHEN_i_<_2_THEN_i_ELSE_'two'_END_FROM_mytable",
		"SELECT_CASE_WHEN_i_>_2_THEN_'more_than_two'_WHEN_i_<_2_THEN_'less_than_two'_ELSE_2_END_FROM_mytable",
		"SELECT_substring(mytable.s,_1,_5)_AS_s_FROM_mytable_INNER_JOIN_othertable_ON_(substring(mytable.s,_1,_5)_=_SUBSTRING(othertable.s2,_1,_5))_GROUP_BY_1_HAVING_s_=_\"secon\"",
		"SELECT_s,_i_FROM_mytable_GROUP_BY_i_ORDER_BY_SUBSTRING(s,_1,_1)_DESC",
		"SELECT_s,_i_FROM_mytable_GROUP_BY_i_HAVING_count(*)_>_0_ORDER_BY_SUBSTRING(s,_1,_1)_DESC",
		"SELECT_GREATEST(i,_s)_FROM_mytable",
		"select_date_format(da,_'%s')_from_typestable_order_by_1",
		"select_md5(i)_from_mytable_order_by_1",
		"select_sha1(i)_from_mytable_order_by_1",
		"select_sha2(i,_256)_from_mytable_order_by_1",
		"select_octet_length(s)_from_mytable_order_by_i",
		"select_char_length(s)_from_mytable_order_by_i",
		"select_locate(upper(\"roW\"),_upper(s),_power(10,_0))_from_mytable_order_by_i",
		"select_log2(i)_from_mytable_order_by_i",
		"select_ln(i)_from_mytable_order_by_i",
		"select_log10(i)_from_mytable_order_by_i",
		"select_log(3,_i)_from_mytable_order_by_i",
		"SELECT_LEAST(i,_s)_FROM_mytable",
		"SELECT_i_FROM_mytable_WHERE_NOT_s_ORDER_BY_1_DESC",
		"SELECT_i_FROM_mytable_WHERE_i_=_(SELECT_1)",
		"SELECT_i_FROM_mytable_mt________WHERE_(SELECT_i_FROM_mytable_where_i_=_mt.i_and_i_>_2)_IS_NOT_NULL________AND_(SELECT_i2_FROM_othertable_where_i2_=_i)_IS_NOT_NULL________ORDER_BY_i",
		"SELECT_i_FROM_mytable_mt________WHERE_(SELECT_i_FROM_mytable_where_i_=_mt.i_and_i_>_1)_IS_NOT_NULL________AND_(SELECT_i2_FROM_othertable_where_i2_=_i_and_i_<_3)_IS_NOT_NULL________ORDER_BY_i",
		"SELECT_i_FROM_mytable_mt________WHERE_(SELECT_i_FROM_mytable_where_i_=_mt.i)_IS_NOT_NULL________AND_(SELECT_i2_FROM_othertable_where_i2_=_i)_IS_NOT_NULL________ORDER_BY_i",
		"SELECT_sum(i)_as_isum,_s_FROM_mytable_GROUP_BY_i_ORDER_BY_isum_ASC_LIMIT_0,_200",
		"SELECT_(SELECT_i_FROM_mytable_ORDER_BY_i_ASC_LIMIT_1)_AS_x",
		"SELECT_(SELECT_s_FROM_mytable_ORDER_BY_i_ASC_LIMIT_1)_AS_x",
		"SELECT_pk,_(SELECT_concat(pk,_pk)_FROM_one_pk_WHERE_pk_<_opk.pk_ORDER_BY_1_DESC_LIMIT_1)_as_strpk_FROM_one_pk_opk_having_strpk_>_\"0\"_ORDER_BY_2",
		"SELECT_pk,_(SELECT_max(pk)_FROM_one_pk_WHERE_pk_<_opk.pk)_AS_x_FROM_one_pk_opk_GROUP_BY_x_ORDER_BY_x",
		"SELECT_pk,_(SELECT_max(pk)_FROM_one_pk_WHERE_pk_<_opk.pk)_AS_x_______FROM_one_pk_opk_WHERE_(SELECT_max(pk)_FROM_one_pk_WHERE_pk_<_opk.pk)_>_0_______GROUP_BY_x_ORDER_BY_x",
		"SELECT_pk,_(SELECT_max(pk)_FROM_one_pk_WHERE_pk_<_opk.pk)_AS_x_______FROM_one_pk_opk_WHERE_(SELECT_max(pk)_FROM_one_pk_WHERE_pk_<_opk.pk)_>_0_______GROUP_BY_(SELECT_max(pk)_FROM_one_pk_WHERE_pk_<_opk.pk)_ORDER_BY_x",
		"SELECT_pk,_______(SELECT_sum(pk1+pk2)_FROM_two_pk_WHERE_pk1+pk2_IN_(SELECT_pk1+pk2_FROM_two_pk_WHERE_pk1+pk2_=_pk))_AS_sum,_______(SELECT_min(pk2)_FROM_two_pk_WHERE_pk2_IN_(SELECT_pk2_FROM_two_pk_WHERE_pk2_=_pk))_AS_equal_______FROM_one_pk_ORDER_BY_pk;",
		"SELECT_pk,_______(SELECT_sum(c1)_FROM_two_pk_WHERE_c1_+_3_IN_(SELECT_c4_FROM_two_pk_WHERE_c3_>_opk.c5))_AS_sum,_______(SELECT_sum(c1)_FROM_two_pk_WHERE_pk2_IN_(SELECT_pk2_FROM_two_pk_WHERE_c1_+_1_<_opk.c2))_AS_sum2______FROM_one_pk_opk_ORDER_BY_pk",
		"SELECT_DISTINCT_n_FROM_bigtable_ORDER_BY_t",
		"SELECT_GREATEST(CAST(i_AS_CHAR),_CAST(b_AS_CHAR))_FROM_niltable_order_by_i",
		"SELECT_count(*)_FROM_people_WHERE_last_name='doe'_and_first_name='jane'_order_by_dob",
		"SELECT_VALUES(i)_FROM_mytable",
		"select_i,_row_number()_over_(order_by_i_desc)_+_3,____row_number()_over_(order_by_length(s),i)_+_0.0_/_row_number()_over_(order_by_length(s)_desc,i_desc)_+_0.0____from_mytable_order_by_1;",
		"SELECT_pk,_row_number()_over_(partition_by_v2_order_by_pk_),_max(v3)_over_(partition_by_v2_order_by_pk)_FROM_one_pk_three_idx_ORDER_BY_pk",
		"SELECT_CONVERT_TZ(datetime_col,_\"+00:00\",_\"+04:00\")_FROM_datetime_table_WHERE_i_=_1",
		"SELECT_2_+_2_WHERE_NOT_EXISTS_(SELECT_pk_FROM_one_pk_WHERE_pk_>_4)",
		"SELECT_2_+_2_WHERE_NOT_EXISTS_(SELECT_*_FROM_one_pk_WHERE_pk_>_4)",
		"SELECT_2_+_2_WHERE_EXISTS_(SELECT_*_FROM_one_pk_WHERE_pk_<_4)",
		"SELECT_distinct_pk1_FROM_two_pk_WHERE_EXISTS_(SELECT_pk_from_one_pk_where_pk_<=_two_pk.pk1)",
		"select_exists_(SELECT_pk1_FROM_two_pk);",
		"SELECT_EXISTS_(SELECT_pk_FROM_one_pk_WHERE_pk_>_4)",
		"select_c1_from_jsontable_where_c1_LIKE_(('%'_OR_'dsads')_OR_'%')",
		"select_c1_from_jsontable_where_c1_LIKE_('%'_OR_NULL)",
		"show_function_status",
		"show_function_status_like_'foo'",
		"show_function_status_where_Db='mydb'",
		"SELECT_CONV(i,_10,_2)_FROM_mytable",
		"select_find_in_set('second_row',_s)_from_mytable;",
		"select_find_in_set(s,_'first_row,second_row,third_row')_from_mytable;",
		"select_i_from_mytable_where_find_in_set(s,_'first_row,second_row,third_row')_=_2;",
		"____SELECT_COUNT(*)__FROM_keyless__WHERE_keyless.c0_IN_(_____WITH_RECURSIVE_cte(depth,_i,_j)_AS_(_______SELECT_0,_T1.c0,_T1.c1_______FROM_keyless_T1_______WHERE_T1.c0_=_0_________UNION_ALL_________SELECT_cte.depth_+_1,_cte.i,_T2.c1_+_1_______FROM_cte,_keyless_T2_______WHERE_cte.depth_=_T2.c0___)_____SELECT_U0.c0___FROM_keyless_U0,_cte___WHERE_cte.j_=_keyless.c0____)_____ORDER_BY_c0;_",
		"____SELECT_COUNT(*)__FROM_keyless__WHERE_keyless.c0_IN_(_____WITH_RECURSIVE_cte(depth,_i,_j)_AS_(_______SELECT_0,_T1.c0,_T1.c1_______FROM_keyless_T1_______WHERE_T1.c0_=_0_________UNION_ALL_________SELECT_cte.depth_+_1,_cte.i,_T2.c1_+_1_______FROM_cte,_keyless_T2_______WHERE_cte.depth_=_T2.c0___)_____SELECT_U0.c0___FROM_cte,_keyless_U0____WHERE_cte.j_=_keyless.c0_____)_____ORDER_BY_c0;_",
		"SELECT_pk1,_SUM(c1)_FROM_two_pk",
		"_select_*_from_mytable,__lateral_(__with_recursive_cte(a)_as_(___select_y_from_xy___union___select_x_from_cte___join___(____select_*_____from_xy____where_x_=_1____)_sqa1___on_x_=_a___limit_3___)__select_*_from_cte_)_sqa2_where_i_=_a_order_by_i;",
		"_select____dayname(id),____dayname(i8),____dayname(i16),____dayname(i32),____dayname(i64),____dayname(u8),____dayname(u16),____dayname(u32),____dayname(u64),____dayname(f32),____dayname(f64),____dayname(ti),____dayname(da),____dayname(te),____dayname(bo),____dayname(js),____dayname(bl),____dayname(e1),____dayname(s1)_from_typestable",
		"select_*_from_mytable_order_by_dayname(i)",
		"select_*_from_mytable_where_(i_BETWEEN_(CASE_1_WHEN_2_THEN_1.0_ELSE_(1||2)_END)_AND_i)",
		"select_*_from_mytable_where_(i_BETWEEN_(''_BETWEEN_''_AND_(''_OR_'#'))_AND_i)",
		"select_*_from_(select_'k'_as_k)_sq_join_bigtable_on_t_=_k_join_xy_where_x_between_n_and_n;",
		"select_*_from_xy_inner_join_uv_on_(xy.x_in_(false_in_('asdf')));",
		"select_length(space(i))_from_mytable;",
		"select_concat(space(i),_'a')_from_mytable;",
		"select_space(i_*_2)_from_mytable;",
		"select_atan(i),_atan2(i,_i_+_2)_from_mytable;",
		"select_elt(i,_'a',_'b')_from_mytable;",
		"select_field(i,_'1',_'2',_'3')_from_mytable;",
		"select_char(i,_i_+_10,_pi())_from_mytable;",
		"select_length(random_bytes(i))_from_mytable;",
		"DESCRIBE_keyless",
		"SHOW_COLUMNS_FROM_keyless",
		"SHOW_FULL_COLUMNS_FROM_keyless",
	}

	// cases lead to panics
	// "SELECT_JSON_KEYS(c3)_FROM_jsontable",
	// "SELECT_(SELECT_s_FROM_mytable_ORDER_BY_i_ASC_LIMIT_1)_AS_x",

)

// TestQueries tests the given queries on an engine under a variety of circumstances:
// 1) Partitioned tables / non partitioned tables
// 2) Mergeable / unmergeable / native / no indexes
// 3) Parallelism on / off
func TestQueries(t *testing.T) {
	t.Skip("wait for fix")
	for _, numPartitions := range numPartitionsVals {
		for _, indexBehavior := range indexBehaviors {
			for _, parallelism := range parallelVals {
				if parallelism == 1 && numPartitions == testNumPartitions && indexBehavior.name == "nativeIndexes" {
					// This case is covered by TestQueriesSimple
					continue
				}
				testName := fmt.Sprintf("partitions=%d,indexes=%v,parallelism=%v", numPartitions, indexBehavior.name, parallelism)
				harness := NewDuckHarness(testName, parallelism, numPartitions, indexBehavior.nativeIndexes, indexBehavior.driverInitializer)

				harness.SetupScriptsToSkip(
					setup.Fk_tblData,     // Skip foreign key setup (not supported)
					setup.TypestableData, // Skip enum/set type setup (not supported)
				)

				harness.QueriesToSkip(notApplicableQueries...)
				harness.QueriesToSkip(waitForFixQueries...)

				t.Run(testName, func(t *testing.T) {
					enginetest.TestQueries(t, harness)
				})
			}
		}
	}
}

// TestQueriesPreparedSimple runs the canonical test queries against a single threaded index enabled harness.
func TestQueriesPreparedSimple(t *testing.T) {
	t.Skip("wait for fix")
	harness := NewDefaultDuckHarness()
	if harness.IsUsingServer() {
		t.Skip("issue: https://github.com/dolthub/dolt/issues/6904 and https://github.com/dolthub/dolt/issues/6901")
	}
	enginetest.TestQueriesPrepared(t, harness)
}

// TestQueriesSimple runs the canonical test queries against a single threaded index enabled harness.
func TestQueriesSimple(t *testing.T) {
	harness := NewDefaultDuckHarness()

	harness.QueriesToSkip(notApplicableQueries...)
	harness.QueriesToSkip(waitForFixQueries...)
	enginetest.TestQueries(t, harness)
}

// TestJoinQueries runs the canonical test queries against a single threaded index enabled harness.
func TestJoinQueries(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestJoinQueries(t, NewDefaultDuckHarness())
}

func TestLateralJoin(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestLateralJoinQueries(t, NewDefaultDuckHarness())
}

// TestJoinPlanning runs join-specific tests for merge
func TestJoinPlanning(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestJoinPlanning(t, NewDefaultDuckHarness())
}

// TestJoinOps runs join-specific tests for merge
func TestJoinOps(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestJoinOps(t, NewDefaultDuckHarness(), enginetest.DefaultJoinOpTests)
}

func TestJoinStats(t *testing.T) {
	t.Skip("wait for fix")
	harness := NewDefaultDuckHarness()
	if harness.IsUsingServer() {
		t.Skip("join stats don't work with bindvars")
	}
	enginetest.TestJoinStats(t, harness)
}

// TestJSONTableQueries runs the canonical test queries against a single threaded index enabled harness.
func TestJSONTableQueries(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestJSONTableQueries(t, NewDefaultDuckHarness())
}

// TestJSONTableScripts runs the canonical test queries against a single threaded index enabled harness.
func TestJSONTableScripts(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestJSONTableScripts(t, NewDefaultDuckHarness())
}

// TestBrokenJSONTableScripts runs the canonical test queries against a single threaded index enabled harness.
func TestBrokenJSONTableScripts(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestBrokenJSONTableScripts(t, enginetest.NewSkippingMemoryHarness())
}

// Convenience test for debugging a single query. Unskip and set to the desired query.
func TestSingleQuery(t *testing.T) {
	t.Skip()
	test := queries.QueryTest{
		Query:    `select a.i,a.f, b.i2 from niltable a left join niltable b on a.i = b.i2 order by a.i`,
		Expected: []sql.Row{{1, nil, nil}, {2, nil, 2}, {3, nil, nil}, {4, 4.0, 4}, {5, 5.0, nil}, {6, 6.0, 6}},
	}

	// fmt.Sprintf("%v", test)
	harness := NewDuckHarness("", 1, testNumPartitions, true, nil)
	// harness.UseServer()
	harness.Setup(setup.MydbData, setup.NiltableData)
	engine, err := harness.NewEngine(t)
	require.NoError(t, err)

	engine.EngineAnalyzer().Debug = true
	engine.EngineAnalyzer().Verbose = true

	enginetest.TestQueryWithEngine(t, harness, engine, test)
}

// Convenience test for debugging a single query. Unskip and set to the desired query.
func TestSingleQueryPrepared(t *testing.T) {
	t.Skip()
	var test = queries.ScriptTest{
		Name:        "renaming views with RENAME TABLE ... TO .. statement",
		SetUpScript: []string{},
		Assertions: []queries.ScriptTestAssertion{
			{
				// Original Issue: https://github.com/dolthub/dolt/issues/5714
				Query: `select 1.0/0.0 from dual`,

				Expected: []sql.Row{
					{4},
				},
			},
		},
	}

	// fmt.Sprintf("%v", test)
	harness := NewDuckHarness("", 1, testNumPartitions, false, nil)
	harness.Setup(setup.KeylessSetup...)
	engine, err := harness.NewEngine(t)
	if err != nil {
		panic(err)
	}

	engine.EngineAnalyzer().Debug = true
	engine.EngineAnalyzer().Verbose = true

	enginetest.TestScriptWithEnginePrepared(t, engine, harness, test)
}

// Convenience test for debugging a single query. Unskip and set to the desired query.
func TestSingleScript(t *testing.T) {
	t.Skip()
	var scripts = []queries.ScriptTest{
		{
			Name:        "test script",
			SetUpScript: []string{},
			Assertions:  []queries.ScriptTestAssertion{},
		},
	}

	for _, test := range scripts {
		harness := NewDuckHarness("", 1, testNumPartitions, true, nil)
		engine, err := harness.NewEngine(t)
		if err != nil {
			panic(err)
		}

		enginetest.TestScriptWithEngine(t, engine, harness, test)
	}
}

func TestUnbuildableIndex(t *testing.T) {
	var scripts = []queries.ScriptTest{
		{
			Name: "Failing index builder still returning correct results",
			SetUpScript: []string{
				"CREATE TABLE mytable2 (i BIGINT PRIMARY KEY, s VARCHAR(20))",
				"CREATE UNIQUE INDEX mytable2_s ON mytable2 (s)",
				fmt.Sprintf("CREATE INDEX mytable2_i_s ON mytable2 (i, s) COMMENT '%s'", memory.CommentPreventingIndexBuilding),
				"INSERT INTO mytable2 VALUES (1, 'first row'), (2, 'second row'), (3, 'third row')",
			},
			Assertions: []queries.ScriptTestAssertion{
				{
					Query: "SELECT i FROM mytable2 WHERE i IN (SELECT i FROM mytable2) ORDER BY i",
					Expected: []sql.Row{
						{1},
						{2},
						{3},
					},
				},
			},
		},
	}

	for _, test := range scripts {
		harness := NewDefaultDuckHarness()
		enginetest.TestScript(t, harness, test)
	}
}

func TestBrokenQueries(t *testing.T) {
	enginetest.TestBrokenQueries(t, NewSkippingDuckHarness())
}

func TestQueryPlanTODOs(t *testing.T) {
	harness := NewSkippingDuckHarness()
	harness.Setup(setup.MydbData, setup.Pk_tablesData, setup.NiltableData)
	e, err := harness.NewEngine(t)
	if err != nil {
		log.Fatal(err)
	}
	for _, tt := range queries.QueryPlanTODOs {
		t.Run(tt.Query, func(t *testing.T) {
			enginetest.TestQueryPlan(t, harness, e, tt)
		})
	}
}

// TODO: implement support for versioned queries
// func TestVersionedQueries(t *testing.T) {
// 	for _, numPartitions := range numPartitionsVals {
// 		for _, indexInit := range indexBehaviors {
// 			for _, parallelism := range parallelVals {
// 				testName := fmt.Sprintf("partitions=%d,indexes=%v,parallelism=%v", numPartitions, indexInit.name, parallelism)
// 				harness := NewMetaHarness(testName, parallelism, numPartitions, indexInit.nativeIndexes, indexInit.driverInitializer)

// 				t.Run(testName, func(t *testing.T) {
// 					enginetest.TestVersionedQueries(t, harness)
// 				})
// 			}
// 		}
// 	}
// }

func TestAnsiQuotesSqlMode(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestAnsiQuotesSqlMode(t, NewDefaultDuckHarness())
}

func TestAnsiQuotesSqlModePrepared(t *testing.T) {
	t.Skip("wait for fix")
	harness := NewDefaultDuckHarness()
	if harness.IsUsingServer() {
		t.Skip("prepared test depend on context for current sql_mode information, but it does not get updated when using ServerEngine")
	}
	enginetest.TestAnsiQuotesSqlModePrepared(t, NewDefaultDuckHarness())
}

// Tests of choosing the correct execution plan independent of result correctness. Mostly useful for confirming that
// the right indexes are being used for joining tables.
func TestQueryPlans(t *testing.T) {
	t.Skip("myduckserver has different query plans")
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
		{"nativeAndMergable", mergableIndexDriver, true},
	}

	for _, indexInit := range indexBehaviors {
		t.Run(indexInit.name, func(t *testing.T) {
			harness := NewDuckHarness(indexInit.name, 1, 2, indexInit.nativeIndexes, indexInit.driverInitializer)
			enginetest.TestQueryPlans(t, harness, queries.PlanTests)
		})
	}
}

func TestSingleQueryPlan(t *testing.T) {
	t.Skip()
	tt := []queries.QueryPlanTest{
		{
			Query: `SELECT mytable.i, selfjoin.i FROM mytable INNER JOIN mytable selfjoin ON mytable.i = selfjoin.i WHERE selfjoin.i IN (SELECT 1 FROM DUAL)`,
			ExpectedPlan: "Project\n" +
				" ├─ columns: [mytable.i:0!null, selfjoin.i:2!null]\n" +
				" └─ SemiJoin\n" +
				"     ├─ MergeJoin\n" +
				"     │   ├─ cmp: Eq\n" +
				"     │   │   ├─ mytable.i:0!null\n" +
				"     │   │   └─ selfjoin.i:2!null\n" +
				"     │   ├─ IndexedTableAccess(mytable)\n" +
				"     │   │   ├─ index: [mytable.i,mytable.s]\n" +
				"     │   │   ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
				"     │   │   ├─ colSet: (1,2)\n" +
				"     │   │   ├─ tableId: 1\n" +
				"     │   │   └─ Table\n" +
				"     │   │       ├─ name: mytable\n" +
				"     │   │       └─ columns: [i s]\n" +
				"     │   └─ Filter\n" +
				"     │       ├─ Eq\n" +
				"     │       │   ├─ selfjoin.i:0!null\n" +
				"     │       │   └─ 1 (tinyint)\n" +
				"     │       └─ TableAlias(selfjoin)\n" +
				"     │           └─ IndexedTableAccess(mytable)\n" +
				"     │               ├─ index: [mytable.i]\n" +
				"     │               ├─ static: [{[1, 1]}]\n" +
				"     │               ├─ colSet: (3,4)\n" +
				"     │               ├─ tableId: 2\n" +
				"     │               └─ Table\n" +
				"     │                   ├─ name: mytable\n" +
				"     │                   └─ columns: [i s]\n" +
				"     └─ Project\n" +
				"         ├─ columns: [1 (tinyint)]\n" +
				"         └─ ProcessTable\n" +
				"             └─ Table\n" +
				"                 ├─ name: \n" +
				"                 └─ columns: []\n" +
				"",
			ExpectedEstimates: "Project\n" +
				" ├─ columns: [mytable.i, selfjoin.i]\n" +
				" └─ SemiJoin (estimated cost=4.515 rows=1)\n" +
				"     ├─ MergeJoin (estimated cost=6.090 rows=3)\n" +
				"     │   ├─ cmp: (mytable.i = selfjoin.i)\n" +
				"     │   ├─ IndexedTableAccess(mytable)\n" +
				"     │   │   ├─ index: [mytable.i,mytable.s]\n" +
				"     │   │   └─ filters: [{[NULL, ∞), [NULL, ∞)}]\n" +
				"     │   └─ Filter\n" +
				"     │       ├─ (selfjoin.i = 1)\n" +
				"     │       └─ TableAlias(selfjoin)\n" +
				"     │           └─ IndexedTableAccess(mytable)\n" +
				"     │               ├─ index: [mytable.i]\n" +
				"     │               └─ filters: [{[1, 1]}]\n" +
				"     └─ Project\n" +
				"         ├─ columns: [1]\n" +
				"         └─ Table\n" +
				"             └─ name: \n" +
				"",
			ExpectedAnalysis: "Project\n" +
				" ├─ columns: [mytable.i, selfjoin.i]\n" +
				" └─ SemiJoin (estimated cost=4.515 rows=1) (actual rows=1 loops=1)\n" +
				"     ├─ MergeJoin (estimated cost=6.090 rows=3) (actual rows=1 loops=1)\n" +
				"     │   ├─ cmp: (mytable.i = selfjoin.i)\n" +
				"     │   ├─ IndexedTableAccess(mytable)\n" +
				"     │   │   ├─ index: [mytable.i,mytable.s]\n" +
				"     │   │   └─ filters: [{[NULL, ∞), [NULL, ∞)}]\n" +
				"     │   └─ Filter\n" +
				"     │       ├─ (selfjoin.i = 1)\n" +
				"     │       └─ TableAlias(selfjoin)\n" +
				"     │           └─ IndexedTableAccess(mytable)\n" +
				"     │               ├─ index: [mytable.i]\n" +
				"     │               └─ filters: [{[1, 1]}]\n" +
				"     └─ Project\n" +
				"         ├─ columns: [1]\n" +
				"         └─ Table\n" +
				"             └─ name: \n" +
				"",
		},
	}

	harness := NewDuckHarness("nativeIndexes", 1, 2, true, nil)
	harness.Setup(setup.PlanSetup...)

	for _, test := range tt {
		t.Run(test.Query, func(t *testing.T) {
			engine, err := harness.NewEngine(t)
			engine.EngineAnalyzer().Verbose = true
			engine.EngineAnalyzer().Debug = true

			require.NoError(t, err)
			enginetest.TestQueryPlan(t, harness, engine, test)
		})
	}
}

func TestIntegrationQueryPlans(t *testing.T) {
	t.Skip("myduckserver has different query plans")
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
	}

	for _, indexInit := range indexBehaviors {
		t.Run(indexInit.name, func(t *testing.T) {
			harness := NewDuckHarness(indexInit.name, 1, 1, indexInit.nativeIndexes, indexInit.driverInitializer)
			enginetest.TestIntegrationPlans(t, harness)
		})
	}
}

func TestImdbQueryPlans(t *testing.T) {
	t.Skip("myduckserver has different query plans")
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
	}

	for _, indexInit := range indexBehaviors {
		t.Run(indexInit.name, func(t *testing.T) {
			harness := NewDuckHarness(indexInit.name, 1, 1, indexInit.nativeIndexes, indexInit.driverInitializer)
			enginetest.TestImdbPlans(t, harness)
		})
	}
}

func TestTpccQueryPlans(t *testing.T) {
	t.Skip("myduckserver has different query plans")
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
	}

	for _, indexInit := range indexBehaviors {
		t.Run(indexInit.name, func(t *testing.T) {
			harness := NewDuckHarness(indexInit.name, 1, 1, indexInit.nativeIndexes, indexInit.driverInitializer)
			enginetest.TestTpccPlans(t, harness)
		})
	}
}

func TestTpchQueryPlans(t *testing.T) {
	t.Skip("myduckserver has different query plans")
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
	}

	for _, indexInit := range indexBehaviors {
		t.Run(indexInit.name, func(t *testing.T) {
			harness := NewDuckHarness(indexInit.name, 1, 1, indexInit.nativeIndexes, indexInit.driverInitializer)
			enginetest.TestTpchPlans(t, harness)
		})
	}
}

func TestTpcdsQueryPlans(t *testing.T) {
	t.Skip("myduckserver has different query plans")
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
	}

	for _, indexInit := range indexBehaviors {
		t.Run(indexInit.name, func(t *testing.T) {
			harness := NewDuckHarness(indexInit.name, 1, 1, indexInit.nativeIndexes, indexInit.driverInitializer)
			enginetest.TestTpcdsPlans(t, harness)
		})
	}
}

func TestIndexQueryPlans(t *testing.T) {
	t.Skip("myduckserver has different query plans")
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
		{"nativeAndMergable", mergableIndexDriver, true},
	}

	for _, indexInit := range indexBehaviors {
		t.Run(indexInit.name, func(t *testing.T) {
			harness := NewDuckHarness(indexInit.name, 1, 2, indexInit.nativeIndexes, indexInit.driverInitializer)
			enginetest.TestIndexQueryPlans(t, harness)
		})
	}
}

func TestParallelismQueries(t *testing.T) {
	enginetest.TestParallelismQueries(t, NewDuckHarness("default", 2, testNumPartitions, true, nil))
}

func TestQueryErrors(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestQueryErrors(t, NewDefaultDuckHarness())
}

func TestInfoSchema(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestInfoSchema(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestMySqlDb(t *testing.T) {
	enginetest.TestMySqlDb(t, NewDefaultDuckHarness())
}

// func TestReadOnlyDatabases(t *testing.T) {
// 	enginetest.TestReadOnlyDatabases(t, NewReadOnlyMetaHarness())
// }

// func TestReadOnlyVersionedQueries(t *testing.T) {
// 	enginetest.TestReadOnlyVersionedQueries(t, NewReadOnlyMetaHarness())
// }

func TestColumnAliases(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestColumnAliases(t, NewDefaultDuckHarness())
}

func TestDerivedTableOuterScopeVisibility(t *testing.T) {
	t.Skip("wait for fix")
	harness := NewDefaultDuckHarness()
	enginetest.TestDerivedTableOuterScopeVisibility(t, harness)
}

func TestOrderByGroupBy(t *testing.T) {
	t.Skip("wait for fix")
	// TODO: window validation expecting error message
	enginetest.TestOrderByGroupBy(t, NewDefaultDuckHarness())
}

func TestAmbiguousColumnResolution(t *testing.T) {
	enginetest.TestAmbiguousColumnResolution(t, NewDefaultDuckHarness())
}

func TestInsertInto(t *testing.T) {
	t.Skip("wait for fix")
	harness := NewDefaultDuckHarness()
	enginetest.TestInsertInto(t, harness)
}

func TestInsertIgnoreInto(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestInsertIgnoreInto(t, NewDefaultDuckHarness())
}

func TestInsertDuplicateKeyKeyless(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestInsertDuplicateKeyKeyless(t, NewDefaultDuckHarness())
}

func TestIgnoreIntoWithDuplicateUniqueKeyKeyless(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestIgnoreIntoWithDuplicateUniqueKeyKeyless(t, NewDefaultDuckHarness())
}

func TestInsertIntoErrors(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestInsertIntoErrors(t, NewDefaultDuckHarness())
}

func TestBrokenInsertScripts(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestBrokenInsertScripts(t, NewDefaultDuckHarness())
}

func TestGeneratedColumns(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestGeneratedColumns(t, NewDefaultDuckHarness())
}

func TestGeneratedColumnPlans(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestGeneratedColumnPlans(t, NewDefaultDuckHarness())
}

func TestSysbenchPlans(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestSysbenchPlans(t, NewDefaultDuckHarness())
}

func TestStatistics(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestStatistics(t, NewDefaultDuckHarness())
}

func TestStatisticIndexFilters(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestStatisticIndexFilters(t, NewDefaultDuckHarness())
}

func TestSpatialInsertInto(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestSpatialInsertInto(t, NewDefaultDuckHarness())
}

func TestLoadData(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestLoadData(t, NewDefaultDuckHarness())
}

func TestLoadDataErrors(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestLoadDataErrors(t, NewDefaultDuckHarness())
}

func TestLoadDataFailing(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestLoadDataFailing(t, NewDefaultDuckHarness())
}

func TestSelectIntoFile(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestSelectIntoFile(t, NewDefaultDuckHarness())
}

func TestReplaceInto(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestReplaceInto(t, NewDefaultDuckHarness())
}

func TestReplaceIntoErrors(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestReplaceIntoErrors(t, NewDefaultDuckHarness())
}

func TestUpdate(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestUpdate(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestUpdateIgnore(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestUpdateIgnore(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestUpdateErrors(t *testing.T) {
	t.Skip("wait for fix")
	// TODO different errors
	enginetest.TestUpdateErrors(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestOnUpdateExprScripts(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestOnUpdateExprScripts(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestSpatialUpdate(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestSpatialUpdate(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestDeleteFromErrors(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestDeleteErrors(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestSpatialDeleteFrom(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestSpatialDelete(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestTruncate(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestTruncate(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestDeleteFrom(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestDelete(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestConvert(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestConvert(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestScripts(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestScripts(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestSpatialScripts(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestSpatialScripts(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestSpatialIndexScripts(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestSpatialIndexScripts(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestSpatialIndexPlans(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestSpatialIndexPlans(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestNumericErrorScripts(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestNumericErrorScripts(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestUserPrivileges(t *testing.T) {
	t.Skip("wait for fix")
	harness := NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver)
	if harness.IsUsingServer() {
		t.Skip("TestUserPrivileges test depend on Context to switch the user to run test queries")
	}
	enginetest.TestUserPrivileges(t, harness)
}

func TestUserAuthentication(t *testing.T) {
	t.Skip("wait for fix")
	harness := NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver)
	if harness.IsUsingServer() {
		t.Skip("TestUserPrivileges test depend on Context to switch the user to run test queries")
	}
	enginetest.TestUserAuthentication(t, harness)
}

func TestPrivilegePersistence(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestPrivilegePersistence(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestComplexIndexQueries(t *testing.T) {
	t.Skip("wait for fix")
	harness := NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver)
	enginetest.TestComplexIndexQueries(t, harness)
}

func TestTriggers(t *testing.T) {
	t.Skip("wait for support")
	enginetest.TestTriggers(t, NewDefaultDuckHarness())
}

func TestShowTriggers(t *testing.T) {
	t.Skip("wait for support")
	enginetest.TestShowTriggers(t, NewDefaultDuckHarness())
}

func TestBrokenTriggers(t *testing.T) {
	t.Skip("wait for support")
	h := enginetest.NewSkippingMemoryHarness()
	for _, script := range queries.BrokenTriggerQueries {
		enginetest.TestScript(t, h, script)
	}
}

func TestStoredProcedures(t *testing.T) {
	t.Skip("wait for support")
	for i, test := range queries.ProcedureLogicTests {
		//TODO: the RowIter returned from a SELECT should not take future changes into account
		if test.Name == "FETCH captures state at OPEN" {
			queries.ProcedureLogicTests[0], queries.ProcedureLogicTests[i] = queries.ProcedureLogicTests[i], queries.ProcedureLogicTests[0]
			queries.ProcedureLogicTests = queries.ProcedureLogicTests[1:]
		}
	}
	enginetest.TestStoredProcedures(t, NewDefaultDuckHarness())
}

func TestEvents(t *testing.T) {
	t.Skip("wait for support")
	enginetest.TestEvents(t, NewDefaultDuckHarness())
}

func TestTriggersErrors(t *testing.T) {
	t.Skip("wait for support")
	enginetest.TestTriggerErrors(t, NewDefaultDuckHarness())
}

func TestCreateTable(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestCreateTable(t, NewDefaultDuckHarness())
}

func TestRowLimit(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestRowLimit(t, NewDefaultDuckHarness())
}

func TestDropTable(t *testing.T) {
	enginetest.TestDropTable(t, NewDefaultDuckHarness())
}

func TestRenameTable(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestRenameTable(t, NewDefaultDuckHarness())
}

func TestRenameColumn(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestRenameColumn(t, NewDefaultDuckHarness())
}

func TestAddColumn(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestAddColumn(t, NewDefaultDuckHarness())
}

func TestModifyColumn(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestModifyColumn(t, NewDefaultDuckHarness())
}

func TestDropColumn(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestDropColumn(t, NewDefaultDuckHarness())
}

func TestDropColumnKeylessTables(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestDropColumnKeylessTables(t, NewDefaultDuckHarness())
}

func TestCreateDatabase(t *testing.T) {
	enginetest.TestCreateDatabase(t, NewDefaultDuckHarness())
}

func TestPkOrdinalsDDL(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestPkOrdinalsDDL(t, NewDefaultDuckHarness())
}

func TestPkOrdinalsDML(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestPkOrdinalsDML(t, NewDefaultDuckHarness())
}

func TestDropDatabase(t *testing.T) {
	enginetest.TestDropDatabase(t, NewDefaultDuckHarness())
}

func TestCreateForeignKeys(t *testing.T) {
	t.Skip("wait for support")
	enginetest.TestCreateForeignKeys(t, NewDefaultDuckHarness())
}

func TestDropForeignKeys(t *testing.T) {
	t.Skip("wait for support")
	enginetest.TestDropForeignKeys(t, NewDefaultDuckHarness())
}

func TestForeignKeys(t *testing.T) {
	t.Skip("wait for support")
	enginetest.TestForeignKeys(t, NewDefaultDuckHarness())
}

func TestFulltextIndexes(t *testing.T) {
	t.Skip("wait for support")
	enginetest.TestFulltextIndexes(t, NewDefaultDuckHarness())
}

func TestCreateCheckConstraints(t *testing.T) {
	t.Skip("wait for support")
	enginetest.TestCreateCheckConstraints(t, NewDefaultDuckHarness())
}

func TestChecksOnInsert(t *testing.T) {
	t.Skip("wait for support")
	enginetest.TestChecksOnInsert(t, NewDefaultDuckHarness())
}

func TestChecksOnUpdate(t *testing.T) {
	t.Skip("wait for support")
	enginetest.TestChecksOnUpdate(t, NewDefaultDuckHarness())
}

func TestDisallowedCheckConstraints(t *testing.T) {
	t.Skip("wait for support")
	enginetest.TestDisallowedCheckConstraints(t, NewDefaultDuckHarness())
}

func TestDropCheckConstraints(t *testing.T) {
	t.Skip("wait for support")
	enginetest.TestDropCheckConstraints(t, NewDefaultDuckHarness())
}

func TestReadOnly(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestReadOnly(t, NewDefaultDuckHarness(), true /* testStoredProcedures */)
}

func TestViews(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestViews(t, NewDefaultDuckHarness())
}

// func TestVersionedViews(t *testing.T) {
// 	enginetest.TestVersionedViews(t, NewDefaultMetaHarness())
// }

func TestNaturalJoin(t *testing.T) {
	enginetest.TestNaturalJoin(t, NewDefaultDuckHarness())
}

func TestWindowFunctions(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestWindowFunctions(t, NewDefaultDuckHarness())
}

func TestWindowRangeFrames(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestWindowRangeFrames(t, NewDefaultDuckHarness())
}

func TestNamedWindows(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestNamedWindows(t, NewDefaultDuckHarness())
}

func TestNaturalJoinEqual(t *testing.T) {
	enginetest.TestNaturalJoinEqual(t, NewDefaultDuckHarness())
}

func TestNaturalJoinDisjoint(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestNaturalJoinDisjoint(t, NewDefaultDuckHarness())
}

func TestInnerNestedInNaturalJoins(t *testing.T) {
	enginetest.TestInnerNestedInNaturalJoins(t, NewDefaultDuckHarness())
}

func TestColumnDefaults(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestColumnDefaults(t, NewDefaultDuckHarness())
}

func TestAlterTable(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestAlterTable(t, NewDefaultDuckHarness())
}

func TestDateParse(t *testing.T) {
	harness := NewDefaultDuckHarness()
	if harness.IsUsingServer() {
		t.Skip("issue: https://github.com/dolthub/dolt/issues/6901")
	}
	enginetest.TestDateParse(t, NewDefaultDuckHarness())
}

func TestJsonScripts(t *testing.T) {
	t.Skip("wait for fix")
	var skippedTests []string = nil
	enginetest.TestJsonScripts(t, NewDefaultDuckHarness(), skippedTests)
}

func TestShowTableStatus(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestShowTableStatus(t, NewDefaultDuckHarness())
}

func TestAddDropPks(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestAddDropPks(t, NewDefaultDuckHarness())
}

func TestAddAutoIncrementColumn(t *testing.T) {
	t.Skip("wait for fix")
	for _, script := range queries.AlterTableAddAutoIncrementScripts {
		enginetest.TestScript(t, NewDefaultDuckHarness(), script)
	}
}

func TestNullRanges(t *testing.T) {
	enginetest.TestNullRanges(t, NewDefaultDuckHarness())
}

func TestBlobs(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestBlobs(t, NewDefaultDuckHarness())
}

func TestIndexes(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestIndexes(t, NewDefaultDuckHarness())
}

func TestIndexPrefix(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestIndexPrefix(t, NewDefaultDuckHarness())
}

func TestPersist(t *testing.T) {
	harness := NewDefaultDuckHarness()
	if harness.IsUsingServer() {
		t.Skip("this test depends on Context, which ServerEngine does not depend on or update the current context")
	}
	newSess := func(_ *sql.Context) sql.PersistableSession {
		ctx := harness.NewSession()
		persistedGlobals := memory.GlobalsMap{}
		memSession := ctx.Session.(*memory.Session).SetGlobals(persistedGlobals)
		return memSession
	}
	enginetest.TestPersist(t, harness, newSess)
}

func TestValidateSession(t *testing.T) {
	count := 0
	incrementValidateCb := func() {
		count++
	}

	harness := NewDefaultDuckHarness()
	if harness.IsUsingServer() {
		t.Skip("It depends on ValidateSession() method call on context")
	}
	newSess := func(ctx *sql.Context) sql.PersistableSession {
		memSession := ctx.Session.(*memory.Session)
		memSession.SetValidationCallback(incrementValidateCb)
		return memSession
	}
	enginetest.TestValidateSession(t, harness, newSess, &count)
}

func TestPrepared(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestPrepared(t, NewDefaultDuckHarness())
}

func TestPreparedInsert(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestPreparedInsert(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestPreparedStatements(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestPreparedStatements(t, NewDefaultDuckHarness())
}

func TestCharsetCollationEngine(t *testing.T) {
	t.Skip("wait for fix")
	harness := NewDefaultDuckHarness()
	if harness.IsUsingServer() {
		// Note: charset introducer needs to be handled with the SQLVal when preparing
		//  e.g. what we do currently for `_utf16'hi'` is `_utf16 :v1` with v1 = "hi", instead of `:v1` with v1 = "_utf16'hi'".
		t.Skip("way we prepare the queries with injectBindVarsAndPrepare() method does not work for ServerEngine test")
	}
	enginetest.TestCharsetCollationEngine(t, harness)
}

func TestCharsetCollationWire(t *testing.T) {
	t.Skip("wait for fix")
	if _, ok := os.LookupEnv("CI_TEST"); !ok {
		t.Skip("Skipping test that requires CI_TEST=true")
	}
	harness := NewDefaultDuckHarness()
	enginetest.TestCharsetCollationWire(t, harness, harness.SessionBuilder())
}

func TestDatabaseCollationWire(t *testing.T) {
	t.Skip("wait for fix")
	if _, ok := os.LookupEnv("CI_TEST"); !ok {
		t.Skip("Skipping test that requires CI_TEST=true")
	}
	harness := NewDefaultDuckHarness()
	enginetest.TestDatabaseCollationWire(t, harness, harness.SessionBuilder())
}

func TestTypesOverWire(t *testing.T) {
	t.Skip("wait for fix")
	if _, ok := os.LookupEnv("CI_TEST"); !ok {
		t.Skip("Skipping test that requires CI_TEST=true")
	}
	harness := NewDefaultDuckHarness()
	enginetest.TestTypesOverWire(t, harness, harness.SessionBuilder())
}

func TestTransactions(t *testing.T) {
	t.Skip("wait for support")
	enginetest.TestTransactionScripts(t, NewSkippingDuckHarness())
}

func mergableIndexDriver(dbs []sql.Database) sql.IndexDriver {
	return memory.NewIndexDriver("mydb", map[string][]sql.DriverIndex{
		"mytable": {
			newMergableIndex(dbs, "mytable",
				expression.NewGetFieldWithTable(0, 1, types.Int64, "db", "mytable", "i", false)),
			newMergableIndex(dbs, "mytable",
				expression.NewGetFieldWithTable(1, 1, types.Text, "db", "mytable", "s", false)),
			newMergableIndex(dbs, "mytable",
				expression.NewGetFieldWithTable(0, 1, types.Int64, "db", "mytable", "i", false),
				expression.NewGetFieldWithTable(1, 1, types.Text, "db", "mytable", "s", false)),
		},
		"othertable": {
			newMergableIndex(dbs, "othertable",
				expression.NewGetFieldWithTable(0, 1, types.Text, "db", "othertable", "s2", false)),
			newMergableIndex(dbs, "othertable",
				expression.NewGetFieldWithTable(1, 1, types.Text, "db", "othertable", "i2", false)),
			newMergableIndex(dbs, "othertable",
				expression.NewGetFieldWithTable(0, 1, types.Text, "db", "othertable", "s2", false),
				expression.NewGetFieldWithTable(1, 1, types.Text, "db", "othertable", "i2", false)),
		},
		"bigtable": {
			newMergableIndex(dbs, "bigtable",
				expression.NewGetFieldWithTable(0, 1, types.Text, "db", "bigtable", "t", false)),
		},
		"floattable": {
			newMergableIndex(dbs, "floattable",
				expression.NewGetFieldWithTable(2, 1, types.Text, "db", "floattable", "f64", false)),
		},
		"niltable": {
			newMergableIndex(dbs, "niltable",
				expression.NewGetFieldWithTable(0, 1, types.Int64, "db", "niltable", "i", false)),
			newMergableIndex(dbs, "niltable",
				expression.NewGetFieldWithTable(1, 1, types.Int64, "db", "niltable", "i2", true)),
		},
		"one_pk": {
			newMergableIndex(dbs, "one_pk",
				expression.NewGetFieldWithTable(0, 1, types.Int8, "db", "one_pk", "pk", false)),
		},
		"two_pk": {
			newMergableIndex(dbs, "two_pk",
				expression.NewGetFieldWithTable(0, 1, types.Int8, "db", "two_pk", "pk1", false),
				expression.NewGetFieldWithTable(1, 1, types.Int8, "db", "two_pk", "pk2", false),
			),
		},
	})
}

func newMergableIndex(dbs []sql.Database, tableName string, exprs ...sql.Expression) *memory.Index {
	db, table := findTable(dbs, tableName)
	if db == nil {
		return nil
	}
	return &memory.Index{
		DB:         db.Name(),
		DriverName: memory.IndexDriverId,
		TableName:  tableName,
		Tbl:        table.(*memory.Table),
		Exprs:      exprs,
	}
}

func findTable(dbs []sql.Database, tableName string) (sql.Database, sql.Table) {
	for _, db := range dbs {
		names, err := db.GetTableNames(sql.NewEmptyContext())
		if err != nil {
			panic(err)
		}
		for _, name := range names {
			if name == tableName {
				table, _, _ := db.GetTableInsensitive(sql.NewEmptyContext(), name)
				return db, table
			}
		}
	}
	return nil, nil
}

func TestSQLLogicTests(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestSQLLogicTests(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

// func TestSQLLogicTestFiles(t *testing.T) {
// 	t.Skip()
// 	h := memharness.NewMemoryHarness(NewDefaultMetaHarness())
// 	paths := []string{
// 		"./sqllogictest/testdata/join/join.txt",
// 		"./sqllogictest/testdata/join/subquery_correlated.txt",
// 	}
// 	logictest.RunTestFiles(h, paths...)
// }
