# -*- coding: utf-8 -*-
# Copyright 2023 Ciena Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import attr
from .types import Int16, Int32, Schema, String, Struct, TaggedFields


@attr.s
class RequestHeader(Struct):
    schema = Schema(
        ('api_key', Int16), ('api_version', Int16), ('correlation_id', Int32), ('client_id', String('utf-8'))
    )


@attr.s
class RequestHeader_v2(Struct):
    schema = Schema(
        ('api_key', Int16),
        ('api_version', Int16),
        ('correlation_id', Int32),
        ('client_id', String('utf-8')),
        ('tags', TaggedFields),
    )


@attr.s
class ResponseHeader(Struct):
    schema = Schema(
        ('correlation_id', Int32),
    )


@attr.s
class ResponseHeader_v2(Struct):
    schema = Schema(
        ('correlation_id', Int32),
        ('tags', TaggedFields),
    )


ResponseHeaders = [ResponseHeader, ResponseHeader_v2]

RequestHeaders = [RequestHeader, RequestHeader_v2]
