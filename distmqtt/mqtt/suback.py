# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.

from distmqtt.mqtt.packet import (
    MQTTPacket,
    MQTTFixedHeader,
    SUBACK,
    PacketIdVariableHeader,
    MQTTPayload,
    MQTTVariableHeader,
)
from distmqtt.errors import DistMQTTException, NoDataException
from distmqtt.adapters import StreamAdapter
from distmqtt.codecs import (
    bytes_to_int,
    int_to_bytes,
    read_or_raise,
    decode_string,
    encode_string,
)


class SubackPayload(MQTTPayload):

    __slots__ = ("return_codes",)

    RETURN_CODE_00 = 0x00
    RETURN_CODE_01 = 0x01
    RETURN_CODE_02 = 0x02
    RETURN_CODE_80 = 0x80

    def __init__(self, return_codes=(), group_keys=[]):
        super().__init__()
        self.return_codes = return_codes
        self.group_keys = group_keys

    def __repr__(self):
        return type(self).__name__ + "(return_codes={0})".format(
            repr(self.return_codes)
        )

    def to_bytes(
        self, fixed_header: MQTTFixedHeader, variable_header: MQTTVariableHeader
    ):
        out = bytearray()
        for pk, k in self.group_keys:
            out.extend(encode_string(pk))
            out.extend(encode_string(k))
        for return_code in self.return_codes:
            out.extend(int_to_bytes(return_code, 1))

        return out

    @classmethod
    async def from_stream(
        cls,
        reader: StreamAdapter,
        fixed_header: MQTTFixedHeader,
        variable_header: MQTTVariableHeader,
    ):
        return_codes = []
        group_keys = []

        bytes_to_read = fixed_header.remaining_length - variable_header.bytes_length

        while len(group_keys) != bytes_to_read:
            pk = await decode_string(reader)
            k = await decode_string(reader)
            group_keys.append((pk, k))
            bytes_to_read -= len(pk.encode("utf-8")) + 2 + len(k.encode("utf-8")) + 2

        for _ in range(0, bytes_to_read):
            try:
                return_code_byte = await read_or_raise(reader, 1)
                return_code = bytes_to_int(return_code_byte)
                return_codes.append(return_code)
            except NoDataException:
                break
        return cls(return_codes, group_keys)


class SubackPacket(MQTTPacket):
    VARIABLE_HEADER = PacketIdVariableHeader
    PAYLOAD = SubackPayload

    def __init__(
        self,
        fixed: MQTTFixedHeader = None,
        variable_header: PacketIdVariableHeader = None,
        payload=None,
    ):
        if fixed is None:
            header = MQTTFixedHeader(SUBACK, 0x00)
        else:
            if fixed.packet_type != SUBACK:
                raise DistMQTTException(
                    "Invalid fixed packet type %s for SubackPacket init"
                    % fixed.packet_type
                )
            header = fixed

        super().__init__(header)
        self.variable_header = variable_header
        self.payload = payload

    @classmethod
    def build(cls, packet_id, return_codes, group_keys):
        variable_header = cls.VARIABLE_HEADER(packet_id)
        payload = cls.PAYLOAD(return_codes, group_keys)
        return cls(variable_header=variable_header, payload=payload)
