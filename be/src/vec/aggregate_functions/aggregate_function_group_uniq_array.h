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

// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/AggregateFunctionGroupUniqArray.h
// and modified by Doris

#pragma once

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_array.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/io/var_int.h"