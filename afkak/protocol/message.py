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
import zlib
from enum import Enum
from typing import Optional, Iterator, Tuple, List

import attr

from .types import Struct, Int8, Int64, Bytes, Schema, UInt32, Int32, UnsignedVarInt32
from ..codec import gzip_decode
from ..common import Message, OffsetAndMessage, ConsumerFetchSizeTooSmall


class Codec(Enum):
    """
    The codec used to compress messages.
    """

    GZIP = 0x01
    SNAPPY = 0x02
    LZ4 = 0x03
    ZSTD = 0x04


@attr.s
class MessageBuilder(Struct):
    crc: int = attr.ib()
    magic: int = attr.ib()
    attributes: int = attr.ib()
    key: bytes = attr.ib()
    value: bytes = attr.ib()
    timestamp: Optional[int] = attr.ib(default=None)


class MsgEncodeHandler:
    @classmethod
    def encode(cls, message: Message) -> bytes:
        """
        Encode a message to bytes.
        """
        if message.magic == 1:
            if message.attributes is None:
                message.attributes = 0  # hack to handle legacy code

            msg_fields = (message.magic, message.attributes, message.timestamp, message.key, message.value)
            encoded = Message_v1.schema.encode(msg_fields)
            # calculate the crc after the message is built
            crc = zlib.crc32(encoded) & 0xFFFFFFFF  # Ensure unsigned
            crc_encoded = UInt32.encode(crc)
            msg = crc_encoded + encoded

            return msg
        else:
            if message.attributes is None:  # hack to handle legacy code
                message.attributes = 0

            msg_fields = (message.magic, message.attributes, message.key, message.value)
            encoded = Message_v0.schema.encode(msg_fields)
            crc = zlib.crc32(encoded) & 0xFFFFFFFF  # Ensure unsigned
            crc_encoded = UInt32.encode(crc)
            msg = crc_encoded + encoded

            return msg


class MsgDecodeHandler:
    def decode(self, message: bytes) -> Message:
        """
        Decode a message from bytes.
        """
        _validated_crc = zlib.crc32(message[4:]) & 0xFFFFFFFF  # Ensure unsigned
        if isinstance(message, bytes):
            message = io.BytesIO(message)

        # message defaults (these are the same for magic 0, 1, and 2)
        crc, magic, attr = UInt32.decode(message), Int8.decode(message), Int8.decode(message)
        attributes = attr & 0x07

        remaining = Messages[magic].schema.fields[2:]  # crc is not included in the schema
        fields = [f.decode(message) for f in remaining]
        if magic == 1:
            timestamp = fields[0]
        else:
            timestamp = None
        if _validated_crc != crc:
            raise ValueError("CRC validation failed")
        if attributes != 0:
            msgs = []
            decompressed = self.decompress(attributes, fields[-1])
            OffsetAndMessage = MessageSetBuilder().decode(decompressed, len(decompressed))
            for offset, msg in OffsetAndMessage:
                msgs.append(msg.value)
            value = msgs
            return Message(value=value, key=fields[-2], magic=magic, attributes=attributes, timestamp=timestamp)

        else:
            value = fields[-1]
            return Message(value=value, key=fields[-2], magic=magic, attributes=attributes, timestamp=timestamp)

    def decompress(self, attributes: int, value: bytes) -> bytes:
        """
        Decompress a message from bytes.
        """

        codec = attributes & 0x07
        print(f'Compressed message codec: {codec}')
        if codec == 1:
            value = gzip_decode(value)
            print(f'GZIP decompressed message: {value}')
        elif codec == Codec.SNAPPY.value:
            raise NotImplementedError("Snappy decompression not implemented")
        elif codec == Codec.LZ4.value:
            raise NotImplementedError("LZ4 decompression not implemented")
        elif codec == Codec.ZSTD.value:
            raise NotImplementedError("ZSTD decompression not implemented")
        else:
            raise ValueError("Unknown codec: %d" % codec)
        return value


# fmt: off
@attr.s
class Message_v0(Struct):
    encoder = MsgEncodeHandler
    decoder = MsgDecodeHandler
    schema = Schema(('magic', Int8), ('attributes', Int8), ('key', Bytes), ('value', Bytes))


@attr.s
class Message_v1(Struct):
    encoder = MsgEncodeHandler
    decoder = MsgDecodeHandler
    schema = Schema(('magic', Int8), ('attributes', Int8), ('timestamp', Int64), ('key', Bytes), ('value', Bytes))


@attr.s(slots=True)
class MessageSet(Struct):
    schema = Schema(('offset', Int64), ('message', Bytes))

# fmt: on


@attr.s(slots=True)
class MessageSetBuilder(Struct):
    offset: int = attr.ib(default=None)
    message: List[Message] = attr.ib(default=attr.Factory(list))

    def encode(self) -> bytes:
        """
        Encode a message set to bytes.
        """
        message_set = []
        incr = 1
        if self.offset is None:
            incr = 0
            self.offset = 0
        for msg in self.message:
            message_set.append(MessageSet.schema.encode((self.offset, MsgEncodeHandler.encode(msg))))
            self.offset += incr
        return b''.join(message_set)

    def decode(self, data: bytes, bytes_to_read=None) -> Iterator[OffsetAndMessage]:
        """
        Iteratively decode a MessageSet

        Reads repeated elements of (offset, message), calling decode_message
        to decode a single message. Since compressed messages contain futher
        MessageSets, these two methods have been decoupled so that they may
        recurse easily.

        :param data: The data to decode
        :param bytes_to_read: The number of bytes to read otherwise we default to reading the first Int32
        """
        if isinstance(data, bytes):
            data = io.BytesIO(data)
        if bytes_to_read is None:
            bytes_to_read = Int32.decode(data)

        # create an internal buffer to avoid over-reading
        raw = io.BytesIO(data.read(bytes_to_read))

        while bytes_to_read:
            try:
                offset = Int64.decode(raw)
                msg_bytes = Bytes.decode(raw)
                bytes_to_read -= 12 + len(msg_bytes)  # 12 is the size of the offset and message size fields (header)
                yield OffsetAndMessage(offset, MsgDecodeHandler().decode(msg_bytes))
            except ValueError:
                # If we get a value error here, it means we have read past the end of the buffer
                # This can happen if the FetchRequest max_bytes is smaller than the available message set
                # The server will return partial data for the final message
                # So we throw an exception here to indicate the fetch size is too small
                raise ConsumerFetchSizeTooSmall()


Messages = [Message_v0, Message_v1]
