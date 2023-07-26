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
from .types import Array, Bytes, Int8, Int16, Int32, Int64, Schema, String, Struct


@attr.s
class FetchRequestResponsePartitionAbortedTransaction:
    """
    This is a wrapper class for FetchResponse_v<version>. Its used to reconstruct the
    decoded response from the server to a build a response object that can be used
    """

    producer_id: int = attr.ib()
    first_offset: int = attr.ib()


def _convert_to_fetch_request_response_partition_aborted_transaction(aborted_transaction):
    return [
        FetchRequestResponsePartitionAbortedTransaction(**aborted_transaction)
        for aborted_transaction in aborted_transaction
    ]


@attr.s
class FetchRequestResponseTopicPartition:
    """
    This is a wrapper class for FetchResponse_v<version>. Its used to reconstruct the
    decoded response from the server to a build a response object that can be used
    """

    partition: int = attr.ib()
    error_code: int = attr.ib()
    highwater_offset: int = attr.ib()
    last_stable_offset: int = attr.ib(default=-1)
    log_start_offset: int = attr.ib(default=-1)
    aborted_transactions: list[FetchRequestResponsePartitionAbortedTransaction] = attr.ib(
        default=attr.Factory(list), converter=_convert_to_fetch_request_response_partition_aborted_transaction
    )
    preferred_read_replica: int = attr.ib(default=-1)
    message_set: bytes = attr.ib(default=attr.Factory(bytes))


def _convert_to_fetch_request_response_topic_partition(topic_partition):
    return [FetchRequestResponseTopicPartition(**topic_partition) for topic_partition in topic_partition]


@attr.s
class FetchRequestResponseTopic:
    """
    This is a wrapper class for FetchResponse_v<version>. Its used to reconstruct the
    decoded response from the server to a build a response object that can be used
    """

    topic: str = attr.ib()
    partitions: list[FetchRequestResponseTopicPartition] = attr.ib(
        default=attr.Factory(list), converter=_convert_to_fetch_request_response_topic_partition
    )


def _convert_to_fetch_request_response_topic(topic):
    return [FetchRequestResponseTopic(**topic_entry) for topic_entry in topic]


@attr.s
class FetchRequestResponse:
    """
    This is a wrapper class for FetchResponse_v<version>. Its used to reconstruct the
    decoded response from the server to a build a response object that can be used
    """

    throttle_time_ms: int = attr.ib(default=0)
    error_code: int = attr.ib(default=0)
    session_id: int = attr.ib(default=0)
    topics: list[FetchRequestResponseTopic] = attr.ib(
        default=attr.Factory(list), converter=_convert_to_fetch_request_response_topic
    )


@attr.s
class FetchRequestBuilderTopicPartition(Struct):
    """
    This is a wrapper class for FetchRequest_v<version>. Its used to construct a request object
    that can be encoded and sent to the server
    """

    partition: int = attr.ib()
    offset: int = attr.ib()
    max_bytes: int = attr.ib()


@attr.s
class FetchRequestBuilderTopicPartitionV5(Struct):
    """
    V5 adds log_start_offset to the request
    """

    partition: int = attr.ib()
    offset: int = attr.ib()
    log_start_offset: int = attr.ib()
    max_bytes: int = attr.ib()


@attr.s
class FetchRequestBuilderTopicPartitionV9(Struct):
    """
    V9 adds current_leader_epoch to the request
    """

    partition: int = attr.ib()
    current_leader_epoch: int = attr.ib()
    fetch_offset: int = attr.ib()
    log_start_offset: int = attr.ib()
    max_bytes: int = attr.ib()


@attr.s
class FetchRequestBuilderTopicPartitionV12(Struct):
    """
    V12 adds last_fetched_epoch to the request
    """

    partition: int = attr.ib()
    current_leader_epoch: int = attr.ib()
    fetch_offset: int = attr.ib()
    last_fetched_epoch: int = attr.ib()
    log_start_offset: int = attr.ib()
    max_bytes: int = attr.ib()


@attr.s
class FetchRequestBuilderTopic(Struct):
    """
    This is a wrapper class for FetchRequest_v<version>. Its used to construct a request object
    that can be encoded and sent to the server
    """

    topic: str = attr.ib()
    partitions: list[FetchRequestBuilderTopicPartition] = attr.ib(default=attr.Factory(list))


@attr.s
class FetchRequestBuilder(Struct):
    """
    This is a wrapper class for FetchRequest_v<version>. Its used to construct a request object
    that can be encoded and sent to the server
    """

    replica_id: int = attr.ib()
    max_wait_time: int = attr.ib()
    min_bytes: int = attr.ib()
    topics: list[FetchRequestResponseTopic] = attr.ib(default=attr.Factory(list))


@attr.s
class FetchRequestBuilderV3(FetchRequestBuilder):
    """
    V3 adds max_bytes to the request
    """

    max_bytes: int = attr.ib(default=-1)


@attr.s
class FetchRequestBuilderV4(FetchRequestBuilderV3):
    """
    V4 adds isolation_level to the request
    """

    isolation_level: int = attr.ib(default=0)


@attr.s
class FetchRequestBuilderV5(FetchRequestBuilderV4):
    """
    V5 adds session_id and session_epoch to the request
    """

    session_id: int = attr.ib(default=0)
    session_epoch: int = attr.ib(default=0)


@attr.s
class FetchRequestBuilderV7(FetchRequestBuilderV5):
    """
    V7 adds forgotten_topics_data to the request
    """

    forgotten_topics_data: list[FetchRequestBuilderTopic] = attr.ib(default=attr.Factory(list))


@attr.s
class FetchRequestBuilderV9(FetchRequestBuilderV7):
    """
    V9 adds rack_id to the request
    """

    rack_id: str = attr.ib(default="")


# fmt: off

@attr.s
class FetchResponse_v0(Struct):
    resp_handler = FetchRequestResponse
    schema = Schema(
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('error_code', Int16),
                ('highwater_offset', Int64),
                ('message_set', Bytes)))))
    )


@attr.s
class FetchResponse_v1(Struct):
    resp_handler = FetchRequestResponse
    schema = Schema(
        ('throttle_time_ms', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('error_code', Int16),
                ('highwater_offset', Int64),
                ('message_set', Bytes)))))
    )


@attr.s
class FetchResponse_v2(Struct):
    resp_handler = FetchRequestResponse
    schema = FetchResponse_v1.schema


@attr.s
class FetchResponse_v3(Struct):
    resp_handler = FetchRequestResponse
    schema = FetchResponse_v2.schema


@attr.s
class FetchResponse_v4(Struct):
    resp_handler = FetchRequestResponse
    schema = Schema(
        ('throttle_time_ms', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('error_code', Int16),
                ('highwater_offset', Int64),
                ('last_stable_offset', Int64),
                ('aborted_transactions', Array(
                    ('producer_id', Int64),
                    ('first_offset', Int64))),
                ('message_set', Bytes)))))
    )


@attr.s
class FetchResponse_v5(Struct):
    resp_handler = FetchRequestResponse
    schema = Schema(
        ('throttle_time_ms', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('error_code', Int16),
                ('highwater_offset', Int64),
                ('last_stable_offset', Int64),
                ('log_start_offset', Int64),
                ('aborted_transactions', Array(
                    ('producer_id', Int64),
                    ('first_offset', Int64))),
                ('message_set', Bytes)))))
    )


@attr.s
class FetchResponse_v6(Struct):
    resp_handler = FetchRequestResponse
    schema = FetchResponse_v5.schema


@attr.s
class FetchResponse_v7(Struct):
    """
    Add error_code and session_id to response
    """
    resp_handler = FetchRequestResponse
    schema = Schema(
        ('throttle_time_ms', Int32),
        ('error_code', Int16),
        ('session_id', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('error_code', Int16),
                ('highwater_offset', Int64),
                ('last_stable_offset', Int64),
                ('log_start_offset', Int64),
                ('aborted_transactions', Array(
                    ('producer_id', Int64),
                    ('first_offset', Int64))),
                ('message_set', Bytes)))))
    )


@attr.s
class FetchResponse_v8(Struct):
    resp_handler = FetchRequestResponse
    schema = FetchResponse_v7.schema


@attr.s
class FetchResponse_v9(Struct):
    resp_handler = FetchRequestResponse
    schema = FetchResponse_v7.schema


@attr.s
class FetchResponse_v10(Struct):
    resp_handler = FetchRequestResponse
    schema = FetchResponse_v7.schema


@attr.s
class FetchResponse_v11(Struct):
    resp_handler = FetchRequestResponse
    schema = Schema(
        ('throttle_time_ms', Int32),
        ('error_code', Int16),
        ('session_id', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('error_code', Int16),
                ('highwater_offset', Int64),
                ('last_stable_offset', Int64),
                ('log_start_offset', Int64),
                ('aborted_transactions', Array(
                    ('producer_id', Int64),
                    ('first_offset', Int64))),
                ('preferred_read_replica', Int32),
                ('message_set', Bytes)))))
    )


@attr.s
class FetchResponse_v12(Struct):
    resp_handler = FetchRequestResponse
    schema = FetchResponse_v11.schema


@attr.s
class FetchResponse_v13(Struct):
    resp_handler = FetchRequestResponse
    schema = FetchResponse_v11.schema


@attr.s
class FetchResponse_v14(Struct):
    resp_handler = FetchRequestResponse
    schema = FetchResponse_v11.schema


@attr.s
class FetchResponse_v15(Struct):
    resp_handler = FetchRequestResponse
    schema = FetchResponse_v11.schema


@attr.s
class FetchRequest_v0(Struct):
    schema = Schema(
        ('replica_id', Int32),
        ('max_wait_time', Int32),
        ('min_bytes', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('offset', Int64),
                ('max_bytes', Int32)))))
    )


@attr.s
class FetchRequest_v1(Struct):
    schema = FetchRequest_v0.schema


@attr.s
class FetchRequest_v2(Struct):
    schema = FetchRequest_v1.schema


@attr.s
class FetchRequest_v3(Struct):
    schema = Schema(
        ('replica_id', Int32),
        ('max_wait_time', Int32),
        ('min_bytes', Int32),
        ('max_bytes', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('offset', Int64),
                ('max_bytes', Int32)))))
    )


@attr.s
class FetchRequest_v4(Struct):
    schema = Schema(
        ('replica_id', Int32),
        ('max_wait_time', Int32),
        ('min_bytes', Int32),
        ('max_bytes', Int32),
        ('isolation_level', Int8),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('offset', Int64),
                ('max_bytes', Int32)))))
    )


@attr.s
class FetchRequest_v5(Struct):
    schema = Schema(
        ('replica_id', Int32),
        ('max_wait_time', Int32),
        ('min_bytes', Int32),
        ('max_bytes', Int32),
        ('isolation_level', Int8),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('fetch_offset', Int64),
                ('log_start_offset', Int64),
                ('max_bytes', Int32)))))
    )


@attr.s
class FetchRequest_v6(Struct):
    schema = FetchRequest_v5.schema


@attr.s
class FetchRequest_v7(Struct):
    schema = Schema(
        ('replica_id', Int32),
        ('max_wait_time', Int32),
        ('min_bytes', Int32),
        ('max_bytes', Int32),
        ('isolation_level', Int8),
        ('session_id', Int32),
        ('session_epoch', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('fetch_offset', Int64),
                ('log_start_offset', Int64),
                ('max_bytes', Int32))))),
        ('forgotten_topics_data', Array(
            ('topic', String),
            ('partitions', Array(Int32))
        )),
    )


@attr.s
class FetchRequest_v8(Struct):
    schema = FetchRequest_v7.schema


@attr.s
class FetchRequest_v9(Struct):
    schema = Schema(
        ('replica_id', Int32),
        ('max_wait_time', Int32),
        ('min_bytes', Int32),
        ('max_bytes', Int32),
        ('isolation_level', Int8),
        ('session_id', Int32),
        ('session_epoch', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('current_leader_epoch', Int32),
                ('fetch_offset', Int64),
                ('log_start_offset', Int64),
                ('max_bytes', Int32))))),
        ('forgotten_topics_data', Array(
            ('topic', String),
            ('partitions', Array(Int32)),
        )),
    )


@attr.s
class FetchRequest_v10(Struct):
    schema = FetchRequest_v9.schema


@attr.s
class FetchRequest_v11(Struct):
    schema = Schema(
        ('replica_id', Int32),
        ('max_wait_time', Int32),
        ('min_bytes', Int32),
        ('max_bytes', Int32),
        ('isolation_level', Int8),
        ('session_id', Int32),
        ('session_epoch', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('current_leader_epoch', Int32),
                ('fetch_offset', Int64),
                ('log_start_offset', Int64),
                ('max_bytes', Int32))))),
        ('forgotten_topics_data', Array(
            ('topic', String),
            ('partitions', Array(Int32))
        )),
        ('rack_id', String('utf-8')),
    )


@attr.s
class FetchRequest_v12(Struct):
    schema = Schema(
        ('replica_id', Int32),
        ('max_wait_time', Int32),
        ('min_bytes', Int32),
        ('max_bytes', Int32),
        ('isolation_level', Int8),
        ('session_id', Int32),
        ('session_epoch', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('current_leader_epoch', Int32),
                ('fetch_offset', Int64),
                ('last_fetched_epoch', Int32),
                ('log_start_offset', Int64),
                ('max_bytes', Int32))))),
        ('forgotten_topics_data', Array(
            ('topic', String),
            ('partitions', Array(Int32))
        )),
        ('rack_id', String('utf-8')),
    )


@attr.s
class FetchRequest_v13(Struct):
    schema = FetchRequest_v11.schema


@attr.s
class FetchRequest_v14(Struct):
    schema = FetchRequest_v11.schema


@attr.s
class FetchRequest_v15(Struct):
    schema = FetchRequest_v11.schema

# fmt: on

FetchRequests = [
    FetchRequest_v0,
    FetchRequest_v1,
    FetchRequest_v2,
    FetchRequest_v3,
    FetchRequest_v4,
    FetchRequest_v5,
    FetchRequest_v6,
    FetchRequest_v7,
    FetchRequest_v8,
    FetchRequest_v9,
    FetchRequest_v10,
    FetchRequest_v11,
]
FetchResponses = [
    FetchResponse_v0,
    FetchResponse_v1,
    FetchResponse_v2,
    FetchResponse_v3,
    FetchResponse_v4,
    FetchResponse_v5,
    FetchResponse_v6,
    FetchResponse_v7,
    FetchResponse_v8,
    FetchResponse_v9,
    FetchResponse_v10,
    FetchResponse_v11,
]
