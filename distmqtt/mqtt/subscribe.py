# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import anyio

from distmqtt.mqtt.packet import (
    MQTTPacket,
    MQTTFixedHeader,
    SUBSCRIBE,
    MQTTPayload,
    MQTTVariableHeader,
)
from distmqtt.errors import DistMQTTException, NoDataException
from distmqtt.codecs import (
    bytes_to_int,
    decode_string,
    decode_packet_id,
    encode_string,
    int_to_bytes,
    read_or_raise,
)


class SubscribeHeader(MQTTVariableHeader):

    __slots__ = ("packet_id", "is_subs")

    def __init__(self, packet_id, is_subs):
        super().__init__()
        self.packet_id = packet_id
        self.is_subs = is_subs

    def to_bytes(self):
        out = b""
        out += int_to_bytes(self.packet_id, 2)
        out += int_to_bytes(self.is_subs, 1)
        return out

    @classmethod
    async def from_stream(cls, reader, fixed_header: MQTTFixedHeader):
        packet_id = await decode_packet_id(reader)
        is_subs_byte = await read_or_raise(reader, 1)
        is_subs = bytes_to_int(is_subs_byte)
        return cls(packet_id, is_subs)

    def __repr__(self):
        return type(self).__name__ + "(packet_id={0}, is_subs={1})".format(
            self.packet_id, self.is_subs
        )


class SubscribePayload(MQTTPayload):

    __slots__ = ("topics",)

    def __init__(self, topics=()):
        super().__init__()
        self.topics = topics

    def to_bytes(
        self, fixed_header: MQTTFixedHeader, variable_header: MQTTVariableHeader
    ):
        out = b""
        for topic in self.topics:
            out += encode_string(topic[0])
            out += int_to_bytes(topic[1], 1)
        return out

    @classmethod
    async def from_stream(
        cls,
        reader: anyio.abc.ByteStream,
        fixed_header: MQTTFixedHeader,
        variable_header: MQTTVariableHeader,
    ):
        topics = []
        payload_length = fixed_header.remaining_length - variable_header.bytes_length
        read_bytes = 0
        while read_bytes < payload_length:
            try:
                topic = await decode_string(reader)
                qos_byte = await read_or_raise(reader, 1)
                qos = bytes_to_int(qos_byte)
                topics.append((topic, qos))
                read_bytes += 2 + len(topic.encode("utf-8")) + 1
            except NoDataException:
                break
        return cls(topics)

    def __repr__(self):
        return type(self).__name__ + "(topics={0!r})".format(self.topics)


class SubscribePacket(MQTTPacket):
    VARIABLE_HEADER = SubscribeHeader
    PAYLOAD = SubscribePayload

    def __init__(
        self,
        fixed: MQTTFixedHeader = None,
        variable_header: SubscribeHeader = None,
        payload=None,
    ):
        if fixed is None:
            header = MQTTFixedHeader(SUBSCRIBE, 0x02)  # [MQTT-3.8.1-1]
        else:
            if fixed.packet_type != SUBSCRIBE:
                raise DistMQTTException(
                    "Invalid fixed packet type %s for SubscribePacket init"
                    % fixed.packet_type
                )
            header = fixed

        super().__init__(header)
        self.variable_header = variable_header
        self.payload = payload

    @classmethod
    def build(cls, topics, packet_id, is_subs=True):
        v_header = cls.VARIABLE_HEADER(packet_id, is_subs)
        payload = SubscribePayload(topics)
        return SubscribePacket(variable_header=v_header, payload=payload)
