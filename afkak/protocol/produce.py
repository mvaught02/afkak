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

from afkak.common import BaseStruct

import attr
from .types import Array, Bytes, Int16, Int32, Int64, Schema, String, Struct


@attr.s
class PartitionMessage(BaseStruct):
    """
    Class for handling partition messages.

    :ivar int partition: The partition to which the message is being sent.
    :ivar bytes messages: The message being sent.
    """

    partition: int = attr.ib()
    messages: bytes = attr.ib()


@attr.s
class TopicPartition(BaseStruct):
    """
    Class for handling topic partitions.

    :ivar str topic: The topic to which the message is being sent.
    :ivar list[PartitionMessage] partitions: The partitions to which the message is being sent.
    """

    topic: str = attr.ib()
    partitions: list[PartitionMessage] = attr.ib(default=attr.Factory(list))


@attr.s
class ProduceRequestBuilder(Struct):
    """
    Class for handling produce requests.

    :ivar int acks: The number of acknowledgements the producer requires the leader to have received before
        considering a request complete.
    :ivar int timeout: The time to await a response in ms.
    :ivar list[TopicPartition] topics: The topics to which the message is being sent.
    """

    acks: int = attr.ib()
    timeout: int = attr.ib()
    topics: list[TopicPartition] = attr.ib(default=attr.Factory(list))


@attr.s
class ProduceRequestBuilderV3(Struct):
    """
    Class for handling produce requests with transactional id. This is used for transactional producer. Or Producer
    api version 3 and above.

    :ivar str transactional_id: The transactional id of the producer.
    :ivar int acks: The number of acknowledgements the producer requires the leader to have received before
        considering a request complete.
    :ivar int timeout: The time to await a response in ms.
    :ivar list[TopicPartition] topics: The topics to which the message is being sent.
    """

    transactional_id: str = attr.ib()
    acks: int = attr.ib()
    timeout: int = attr.ib()
    topics: list[TopicPartition] = attr.ib(default=attr.Factory(list))


@attr.s
class ProduceRequestResponseRecordError:
    """
    Class for handling record errors in produce request response.

    :ivar int batch_index: The index of the record batch that failed.
    :ivar str batch_index_error_message: The error message for the record batch that failed.
    """

    batch_index: int = attr.ib()
    batch_index_error_message: str = attr.ib()


def convert_to_produce_request_response_record_error(record_error):
    return [ProduceRequestResponseRecordError(**record_error_entry) for record_error_entry in record_error]


@attr.s
class ProduceRequestResponseTopicPartition:
    """
    Class for handling topic partitions in produce request response.

    :ivar int partition: The partition this response entry corresponds to.
    :ivar int error_code: The error from this partition, if any.
    :ivar int offset: The offset assigned to the first message in the message set appended to this partition.
    :ivar int timestamp: The timestamp assigned to the message.
    :ivar int log_start_offset: The log start offset.
    :ivar list[ProduceRequestResponseRecordError] record_errors: The record errors.
    :ivar str error_message: The error message.
    """

    partition: int = attr.ib()
    error_code: int = attr.ib()
    offset: int = attr.ib()
    timestamp: int = attr.ib(default=-1)
    log_start_offset: int = attr.ib(default=-1)
    record_errors: list[ProduceRequestResponseRecordError] = attr.ib(
        default=attr.Factory(list), converter=convert_to_produce_request_response_record_error
    )
    error_message: str = attr.ib(default="")


def convert_to_produce_request_response_topic_partition(topic_partition):
    return [ProduceRequestResponseTopicPartition(**topic_partition_entry) for topic_partition_entry in topic_partition]


@attr.s
class ProduceRequestResponseTopic:
    """
    Class for handling topics in produce request response.

    :ivar str topic: The topic this response entry corresponds to.
    :ivar list[ProduceRequestResponseTopicPartition] partitions: The partitions this response entry corresponds to.
    """

    topic: str = attr.ib()
    partitions: list[ProduceRequestResponseTopicPartition] = attr.ib(
        default=attr.Factory(list), converter=convert_to_produce_request_response_topic_partition
    )


def convert_to_produce_request_response_topic(topic):
    return [ProduceRequestResponseTopic(**topic_entry) for topic_entry in topic]


@attr.s
class ProduceRequestResponse:
    """
    Class for handling produce request response.

    :ivar list[ProduceRequestResponseTopic] topics: The topics this response entry corresponds to.
    :ivar int throttle_time_ms: The time in milliseconds the response was throttled.
    """

    topics: list[ProduceRequestResponseTopic] = attr.ib(
        default=attr.Factory(list), converter=convert_to_produce_request_response_topic
    )
    throttle_time_ms: int = attr.ib(default=0)


# fmt: off
@attr.s
class ProduceResponse_v0(Struct):
    resp_handler = ProduceRequestResponse
    schema = Schema(
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('error_code', Int16),
                ('offset', Int64)))))
    )


@attr.s
class ProduceResponse_v1(Struct):
    resp_handler = ProduceRequestResponse
    schema = Schema(
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('error_code', Int16),
                ('offset', Int64))))),
        ('throttle_time_ms', Int32)
    )


@attr.s
class ProduceResponse_v2(Struct):
    """
    schema:
    Produce Response (Version: 2) => [responses] throttle_time_ms
      responses => name [partition_responses]
        name => STRING
        partition_responses => index error_code base_offset log_append_time_ms
          index => INT32
          error_code => INT16
          base_offset => INT64
          log_append_time_ms => INT64
        throttle_time_ms => INT32
    """
    resp_handler = ProduceRequestResponse
    schema = Schema(
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('error_code', Int16),
                ('offset', Int64),
                ('timestamp', Int64))))),
        ('throttle_time_ms', Int32)
    )


@attr.s
class ProduceResponse_v3(Struct):
    resp_handler = ProduceRequestResponse
    schema = ProduceResponse_v2.schema


@attr.s
class ProduceResponse_v4(Struct):
    resp_handler = ProduceRequestResponse
    schema = ProduceResponse_v3.schema


@attr.s
class ProduceResponse_v5(Struct):
    resp_handler = ProduceRequestResponse
    schema = Schema(
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('error_code', Int16),
                ('offset', Int64),
                ('timestamp', Int64),
                ('log_start_offset', Int64))))),
        ('throttle_time_ms', Int32)
    )


@attr.s
class ProduceResponse_v6(Struct):
    resp_handler = ProduceRequestResponse
    schema = ProduceResponse_v5.schema


@attr.s
class ProduceResponse_v7(Struct):
    resp_handler = ProduceRequestResponse
    schema = ProduceResponse_v6.schema


@attr.s
class ProduceResponse_v8(Struct):
    resp_handler = ProduceRequestResponse
    schema = Schema(
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('error_code', Int16),
                ('offset', Int64),
                ('timestamp', Int64),
                ('log_start_offset', Int64)),
                ('record_errors', (Array(
                    ('batch_index', Int32),
                    ('batch_index_error_message', String('utf-8'))
                 ))),
                ('error_message', String('utf-8'))
             ))),
        ('throttle_time_ms', Int32)
    )


@attr.s
class ProduceRequest_v0(Struct[ProduceRequestBuilder]):
    schema = Schema(
        ('required_acks', Int16),
        ('timeout', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('messages', Bytes)))))
    )


@attr.s
class ProduceRequest_v1(Struct[ProduceRequestBuilder]):
    schema = ProduceRequest_v0.schema


@attr.s
class ProduceRequest_v2(Struct[ProduceRequestBuilder]):
    schema = ProduceRequest_v1.schema


@attr.s
class ProduceRequest_v3(Struct[ProduceRequestBuilderV3]):
    schema = Schema(
        ('transactional_id', String('utf-8')),
        ('acks', Int16),
        ('timeout', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('messages', Bytes)))))
    )


@attr.s
class ProduceRequest_v4(Struct[ProduceRequestBuilderV3]):
    schema = ProduceRequest_v3.schema


@attr.s
class ProduceRequest_v5(Struct[ProduceRequestBuilderV3]):
    schema = ProduceRequest_v4.schema


@attr.s
class ProduceRequest_v6(Struct[ProduceRequestBuilderV3]):
    schema = ProduceRequest_v5.schema


@attr.s
class ProduceRequest_v7(Struct[ProduceRequestBuilderV3]):
    schema = ProduceRequest_v6.schema


@attr.s
class ProduceRequest_v8(Struct[ProduceRequestBuilderV3]):
    schema = ProduceRequest_v7.schema

# fmt: on

ProduceRequests = [
    ProduceRequest_v0,
    ProduceRequest_v1,
    ProduceRequest_v2,
    ProduceRequest_v3,
    ProduceRequest_v4,
    ProduceRequest_v5,
    ProduceRequest_v6,
    ProduceRequest_v7,
    ProduceRequest_v8,
]
ProduceResponses = [
    ProduceResponse_v0,
    ProduceResponse_v1,
    ProduceResponse_v2,
    ProduceResponse_v3,
    ProduceResponse_v4,
    ProduceResponse_v5,
    ProduceResponse_v6,
    ProduceResponse_v7,
    ProduceResponse_v8,
]
