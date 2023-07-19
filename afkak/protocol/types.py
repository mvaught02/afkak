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

import struct
from typing import Generic, TypeVar

import attr

T = TypeVar('T')


class StructError(Exception):
    pass


class BinaryCoder:
    struct_format = ""

    @classmethod
    def encode(cls, value):
        try:
            return struct.pack(cls.struct_format, value)
        except struct.error as e:
            raise StructError(f"Error when converting value {value!r} to struct format '{cls.struct_format}': {e}")

    @classmethod
    def decode(cls, data):
        try:
            return struct.unpack(cls.struct_format, data.read(struct.calcsize(cls.struct_format)))[0]
        except struct.error as e:
            raise StructError(f"Error when converting data {data!r} to struct format '{cls.struct_format}': {e}")


class Int8(BinaryCoder):
    struct_format = '>b'


class Int16(BinaryCoder):
    struct_format = '>h'


class Int32(BinaryCoder):
    struct_format = '>i'


class Int64(BinaryCoder):
    struct_format = '>q'


class Float64(BinaryCoder):
    struct_format = '>d'


class String:
    def __init__(self, encoding='utf-8'):
        self.encoding = encoding

    def encode(self, value):
        if value is None:
            return Int16.encode(-1)
        value = str(value).encode(self.encoding)
        return Int16.encode(len(value)) + value

    def decode(self, data):
        length = Int16.decode(data)
        if length < 0:
            return None
        value = data.read(length)
        if len(value) != length:
            raise ValueError('Buffer underrun decoding string')
        return value.decode(self.encoding)


class Bytes:
    @classmethod
    def encode(cls, value):
        if value is None:
            return Int32.encode(-1)
        else:
            return Int32.encode(len(value)) + value

    @classmethod
    def decode(cls, data):
        length = Int32.decode(data)
        if length < 0:
            return None
        value = data.read(length)
        if len(value) != length:
            raise ValueError('Buffer underrun decoding Bytes')
        return value

    @classmethod
    def repr(cls, value):
        return repr(value[:100] + b'...' if value is not None and len(value) > 100 else value)


class Boolean(BinaryCoder):
    struct_format = '>?'


class Schema:
    def __init__(self, *fields):
        if fields:
            self.names, self.fields = zip(*fields)
        else:
            self.names, self.fields = (), ()

    def encode(self, item):
        if len(item) != len(self.fields):
            raise ValueError('Item field count does not match Schema')
        return b''.join([field.encode(item[i]) for i, field in enumerate(self.fields)])

    def decode(self, data):
        return tuple([field.decode(data) for field in self.fields])

    def __len__(self):
        return len(self.fields)

    def repr(self, value):
        key_vals = []
        try:
            for i in range(len(self)):
                try:
                    field_val = getattr(value, self.names[i])
                except AttributeError:
                    field_val = value[i]
                key_vals.append('%s=%s' % (self.names[i], self.fields[i].repr(field_val)))
            return '(' + ', '.join(key_vals) + ')'
        except Exception:
            return repr(value)


class Array:
    def __init__(self, *array_of):
        if len(array_of) > 1:
            self.array_of = Schema(*array_of)
        elif len(array_of) == 1:
            self.array_of = array_of[0]
        else:
            raise ValueError('Array instantiated with no array_of type')

    def encode(self, items):
        if items is None:
            return Int32.encode(-1)
        encoded_items = [self.array_of.encode(item) for item in items]
        return b''.join([Int32.encode(len(encoded_items))] + encoded_items)

    def decode(self, data):
        length = Int32.decode(data)
        if length == -1:
            return None
        return [self.array_of.decode(data) for _ in range(length)]

    def repr(self, list_of_items):
        if list_of_items is None:
            return 'NULL'
        return '[' + ', '.join([self.array_of.repr(item) for item in list_of_items]) + ']'


class UnsignedVarInt32:
    @classmethod
    def decode(cls, data):
        value, i = 0, 0
        while True:
            (b,) = struct.unpack('B', data.read(1))
            if not (b & 0x80):
                break
        return value

    @classmethod
    def encode(cls, value):
        value &= 0xFFFFFFFF
        ret = b''
        while (value & 0xFFFFFF80) != 0:
            b = (value & 0x7F) | 0x80
            ret += struct.pack('B', b)
            value >>= 7
        ret += struct.pack('B', value)
        return ret


class VarInt32:
    @classmethod
    def decode(cls, data):
        value = UnsignedVarInt32.decode(data)
        return (value >> 1) ^ -(value & 1)

    @classmethod
    def encode(cls, value):
        # bring it in line with the java binary repr
        value &= 0xFFFFFFFF
        return UnsignedVarInt32.encode((value << 1) ^ (value >> 31))


class VarInt64:
    @classmethod
    def decode(cls, data):
        value, i = 0, 0
        while True:
            b = data.read(1)
            if not (b & 0x80):
                break
        return (value >> 1) ^ -(value & 1)

    @classmethod
    def encode(cls, value):
        # bring it in line with the java binary repr
        value &= 0xFFFFFFFFFFFFFFFF
        v = (value << 1) ^ (value >> 63)
        ret = b''
        while (v & 0xFFFFFFFFFFFFFF80) != 0:
            b = (value & 0x7F) | 0x80
            ret += struct.pack('B', b)
            v >>= 7
        ret += struct.pack('B', v)
        return ret


class CompactString(String):
    def decode(self, data):
        length = UnsignedVarInt32.decode(data) - 1
        if length < 0:
            return None
        value = data.read(length)
        if len(value) != length:
            raise ValueError('Buffer underrun decoding string')
        return value.decode(self.encoding)

    def encode(self, value):
        if value is None:
            return UnsignedVarInt32.encode(0)
        value = str(value).encode(self.encoding)
        return UnsignedVarInt32.encode(len(value) + 1) + value


class TaggedFields:
    @classmethod
    def decode(cls, data):
        num_fields = UnsignedVarInt32.decode(data)
        ret = {}
        if not num_fields:
            return ret
        prev_tag = -1
        for i in range(num_fields):
            tag = UnsignedVarInt32.decode(data)
            if tag <= prev_tag:
                raise ValueError('Invalid or out-of-order tag {}'.format(tag))
            prev_tag = tag
            size = UnsignedVarInt32.decode(data)
            val = data.read(size)
            ret[tag] = val
        return ret

    @classmethod
    def encode(cls, value):
        ret = UnsignedVarInt32.encode(len(value))
        for k, v in value.items():
            # do we allow for other data types ?? It could get complicated really fast
            assert isinstance(v, bytes), 'Value {} is not a byte array'.format(v)
            assert isinstance(k, int) and k > 0, 'Key {} is not a positive integer'.format(k)
            ret += UnsignedVarInt32.encode(k)
            ret += v
        return ret


class CompactBytes:
    @classmethod
    def decode(cls, data):
        length = UnsignedVarInt32.decode(data) - 1
        if length < 0:
            return None
        value = data.read(length)
        if len(value) != length:
            raise ValueError('Buffer underrun decoding Bytes')
        return value

    @classmethod
    def encode(cls, value):
        if value is None:
            return UnsignedVarInt32.encode(0)
        else:
            return UnsignedVarInt32.encode(len(value) + 1) + value


class CompactArray(Array):
    def encode(self, items):
        if items is None:
            return UnsignedVarInt32.encode(0)
        return b''.join([UnsignedVarInt32.encode(len(items) + 1)] + [self.array_of.encode(item) for item in items])

    def decode(self, data):
        length = UnsignedVarInt32.decode(data) - 1
        if length == -1:
            return None
        return [self.array_of.decode(data) for _ in range(length)]


@attr.s
class Struct(Generic[T]):
    schema = Schema()

    @classmethod
    def to_object(cls, data=None):
        ret = _to_object(cls.schema, cls if data is None else data)
        return cls.resp_handler(**ret)

    def get_item(self, name):
        print("schema names: ", self.schema.names)
        if name not in self.schema.names:
            raise KeyError("%s is not in the schema" % name)
        return getattr(self, name)

    def __iter__(self):
        return iter(attr.astuple(self, recurse=False))

    def __getitem__(self, item):
        return attr.astuple(self, recurse=False)[item]

    def __len__(self):
        return len(attr.astuple(self))

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return attr.astuple(self) == attr.astuple(other)
        return NotImplemented

    def __hash__(self):
        return hash(attr.astuple(self))

    def __lt__(self, other):
        if isinstance(other, self.__class__):
            return attr.astuple(self) < attr.astuple(other)
        return NotImplemented


def _to_object(schema, data):
    obj = {}
    for idx, (name, _type) in enumerate(zip(schema.names, schema.fields)):
        if isinstance(_type, Struct):
            val = _to_object(_type.schema, data[idx])
        else:
            val = data[idx]

        if isinstance(_type, Schema):
            obj[name] = _to_object(_type, val)
        elif isinstance(_type, Array):
            if isinstance(_type.array_of, (Array, Schema)):
                obj[name] = [_to_object(_type.array_of, x) for x in val]
            else:
                obj[name] = val
        else:
            obj[name] = val

    return obj
