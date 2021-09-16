# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.

from distmqtt.codecs import (
    bytes_to_int,
    decode_data_with_length,
    decode_string,
    encode_data_with_length,
    encode_string,
    int_to_bytes,
    read_or_raise,
)
from distmqtt.mqtt.packet import (
    MQTTPacket,
    MQTTFixedHeader,
    CONFIRMATION,
    MQTTPayload,
    PacketIdVariableHeader,
)
from distmqtt.errors import DistMQTTException, NoDataException
from distmqtt.adapters import StreamAdapter
from distmqtt.utils import gen_client_id


class ConfirmationPayload(MQTTPayload):
    def __init__(self, verif=None, g_pk=None):
        self.verif = verif
        self.g_pk = g_pk

    def __repr__(self):
        return "ConfirmationPayload x={0} g={1}".format(self.verif, self.g_pk)

    @classmethod
    async def from_stream(
        cls,
        reader: StreamAdapter,
        fixed_header: MQTTFixedHeader,
        variable_header,
    ):
        payload = cls()
        #  Client identifier
        payload.verif = await decode_string(reader)
        payload.g_pk = await decode_string(reader)
        return payload

    def to_bytes(self, fixed_header: MQTTFixedHeader, variable_header):
        out = bytearray()
        out.extend(encode_string(self.verif))
        out.extend(encode_string(self.g_pk))
        return out


class ConfirmationPacket(MQTTPacket):
    VARIABLE_HEADER = None
    PAYLOAD = ConfirmationPayload

    def __init__(
        self,
        fixed=None,
        variable_header=None,
        payload=None,
    ):
        header = MQTTFixedHeader(CONFIRMATION, 0x00)
        super().__init__(header)
        self.variable_header = None
        self.payload = payload

    @classmethod
    def build(cls, verif=None, g_pk=None):
        payload = ConfirmationPayload(verif, g_pk)
        packet = ConfirmationPacket(payload=payload)
        return packet
