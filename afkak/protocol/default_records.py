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
import io
from typing import List, Tuple

import attr

from .types import Struct, Int8, Int64, Bytes, Schema, UInt32, Int32, UnsignedVarInt32
from afkak.protocol.types import Array, String, Int16


@attr.s
class RecordHeaderBase(Struct):
    """
    Base class for all record header types.
    """

    header_key: bytes = attr.ib()
    header_value: bytes = attr.ib()


@attr.s
class RecordBase(Struct):
    """
    Base class for all record types.
    """

    attributes: int = attr.ib()
    timestamp_delta: int = attr.ib()
    offset_delta: int = attr.ib()
    key: bytes = attr.ib()
    value: bytes = attr.ib()
    headers: List[Tuple[bytes, bytes]] = attr.ib(default=attr.Factory(list))


@attr.s
class RecordBatchBase(Struct):
    """
    Base class for all record batch types.
    """

    base_offset: int = attr.ib()
    batch_length: int = attr.ib()
    partition_leader_epoch: int = attr.ib()
    magic: int = attr.ib()
    crc: int = attr.ib()
    attributes: int = attr.ib()
    last_offset_delta: int = attr.ib()
    first_timestamp: int = attr.ib()
    max_timestamp: int = attr.ib()
    producer_id: int = attr.ib()
    producer_epoch: int = attr.ib()
    base_sequence: int = attr.ib()
    records: List[RecordBase] = attr.ib(default=attr.Factory(list))


@attr.s
class Record(Struct):
    schema = Schema(
        ("attributes", Int8),
        ("timestamp_delta", UnsignedVarInt32),
        ("offset_delta", UnsignedVarInt32),
        ("key", Bytes),
        ("value", Bytes),
        ("headers", Array(("header_key", String("utf-8")), ("header_value", Bytes))),
    )


@attr.s
class RecordBatch(Struct):
    schema = Schema(
        ("base_offset", Int64),
        ("batch_length", Int32),
        ("partition_leader_epoch", Int32),
        ("magic", Int8),
        ("crc", Int32),
        ("attributes", Int16),
        ("last_offset_delta", Int32),
        ("first_timestamp", Int64),
        ("max_timestamp", Int64),
        ("producer_id", Int64),
        ("producer_epoch", Int16),
        ("base_sequence", Int32),
        ("records", Array(("length", UnsignedVarInt32), ("record", Bytes))),
    )


@attr.s
class RecordBatchBuilder:
    """
    A builder for RecordBatch objects.
    """

    base_offset: int = attr.ib()
    partition_leader_epoch: int = attr.ib()
    magic: int = attr.ib()
    crc: int = attr.ib()
    attributes: int = attr.ib()
    last_offset_delta: int = attr.ib()
    first_timestamp: int = attr.ib()
    max_timestamp: int = attr.ib()
    producer_id: int = attr.ib()
    producer_epoch: int = attr.ib()
    base_sequence: int = attr.ib()
    records: List[RecordBase] = attr.ib(default=attr.Factory(list))

    @classmethod
    def decode(cls, data: bytes) -> 'RecordBatchBuilder':
        """
        Decode a RecordBatchBuilder from bytes.
        """
        return cls(*RecordBatch.schema.decode(io.BytesIO(data)))

    def encode(self) -> bytes:
        """
        Encode a RecordBatchBuilder to bytes.
        """
        return RecordBatch.schema.encode(attr.astuple(self))

    def append(self, record: RecordBase) -> None:
        """
        Append a record to the batch.
        """
        self.records.append(record)
