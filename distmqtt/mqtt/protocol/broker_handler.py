# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import anyio
from distmqtt.mqtt.protocol.handler import ProtocolHandler
from distmqtt.mqtt.connack import (
    CONNECTION_ACCEPTED,
    UNACCEPTABLE_PROTOCOL_VERSION,
    IDENTIFIER_REJECTED,
    BAD_USERNAME_PASSWORD,
    NOT_AUTHORIZED,
    ConnackPacket,
)
from distmqtt.mqtt.connect import ConnectPacket
from distmqtt.mqtt.pingreq import PingReqPacket
from distmqtt.mqtt.pingresp import PingRespPacket
from distmqtt.mqtt.subscribe import SubscribePacket
from distmqtt.mqtt.suback import SubackPacket
from distmqtt.mqtt.unsubscribe import UnsubscribePacket
from distmqtt.mqtt.unsuback import UnsubackPacket
from distmqtt.mqtt.confirmation import ConfirmationPacket
from distmqtt.utils import (
    format_client_message,
    create_queue,
    ecqv_cert_generate,
    ecqv_pem_pk_extract,
    ecqv_cert_pk_extract,
    ecqv_verify_confirmation,
    ecqv_group_generate,
)
from distmqtt.session import Session
from distmqtt.plugins.manager import PluginManager
from distmqtt.adapters import StreamAdapter
from distmqtt.errors import MQTTException, NoDataException
from .handler import EVENT_MQTT_PACKET_RECEIVED, EVENT_MQTT_PACKET_SENT

import logging

logger = logging.getLogger(__name__)


class BrokerProtocolHandler(ProtocolHandler):
    clean_disconnect = False

    def __init__(self, plugins_manager: PluginManager, session: Session = None):
        super().__init__(plugins_manager, session)
        self._pending_subscriptions = create_queue(9999)
        self._pending_unsubscriptions = create_queue(9999)

    async def handle_write_timeout(self):
        pass

    async def handle_read_timeout(self):
        raise TimeoutError

    async def _handle_disconnect(
        self, disconnect, wait=True
    ):  # pylint: disable=arguments-differ
        self.logger.debug("Client disconnecting")
        self.clean_disconnect = False  # depending on 'disconnect' (if set)
        with anyio.fail_after(2, shield=True):
            if wait:
                with anyio.move_on_after(self.session.keep_alive):
                    await self._reader_stopped.wait()
            await self.stop()

    async def handle_connection_closed(self):
        if not self._disconnecting:
            self._disconnecting = True
            await self._handle_disconnect(None, wait=False)

    async def handle_connect(self, connect: ConnectPacket):
        # Broker handler shouldn't received CONNECT message during messages handling
        # as CONNECT messages are managed by the broker on client connection
        self.logger.error(
            "%s [MQTT-3.1.0-2] %s : CONNECT message received during messages handling",
            self.session.client_id,
            format_client_message(self.session),
        )
        await self.stop()

    async def handle_pingreq(self, pingreq: PingReqPacket):
        await self._send_packet(PingRespPacket.build())

    async def handle_subscribe(self, subscribe: SubscribePacket):
        subscription = {
            "packet_id": subscribe.variable_header.packet_id,
            "topics": subscribe.payload.topics,
        }
        await self._pending_subscriptions.put(subscription)

    async def handle_unsubscribe(
        self, unsubscribe: UnsubscribePacket
    ):  # pylint: disable=arguments-differ
        unsubscription = {
            "packet_id": unsubscribe.variable_header.packet_id,
            "topics": unsubscribe.payload.topics,
        }
        await self._pending_unsubscriptions.put(unsubscription)

    async def get_next_pending_subscription(self):
        subscription = await self._pending_subscriptions.get()
        return subscription

    async def get_next_pending_unsubscription(self):
        unsubscription = await self._pending_unsubscriptions.get()
        return unsubscription

    async def mqtt_acknowledge_subscription(self, packet_id, return_codes):
        suback = SubackPacket.build(packet_id, return_codes)
        await self._send_packet(suback)

    async def mqtt_acknowledge_unsubscription(self, packet_id):
        unsuback = UnsubackPacket.build(packet_id)
        await self._send_packet(unsuback)

    async def mqtt_connack_authorize(self, authorize: bool):
        if authorize:
            # TODO Gen CA here ?
            ca, r = ecqv_cert_generate(
                self.session.ecqv,
                self.session.client_id,
                self.session.pk,
                self.session.capath,
            )
            pk = ecqv_pem_pk_extract(self.session.ecqv, self.session.capath)
            connack = ConnackPacket.build(
                self.session.parent, CONNECTION_ACCEPTED, ca, r, pk
            )
            ca_pk = ecqv_pem_pk_extract(self.session.ecqv, self.session.capath)
            self.session.cert = ecqv_cert_pk_extract(
                self.session.ecqv, self.session.client_id, ca_pk, ca
            )
        else:
            connack = ConnackPacket.build(self.session.parent, NOT_AUTHORIZED)
        await self._send_packet(connack)

    async def mqtt_confirmation_reception(self, stream: StreamAdapter):
        confirmation = await ConfirmationPacket.from_stream(stream)
        self.session.verif = confirmation.payload.verif
        self.session.g = confirmation.payload.g_pk
        return confirmation

    def mqtt_group_generation(self, ids, g_pks, cert_pks, verify_numbers):
        ecqv_verify_confirmation(
            self.session.ecqv, self.session.verif, self.session.cert, self.session.g
        )
        # ecqv_group_generate(
        #     self.session.ecqv, self.session.capath, ids, g_pks, cert_pks, verify_numbers
        # )

    @classmethod
    async def init_from_connect(
        cls, stream: StreamAdapter, plugins_manager, ecqv=None, capath=None
    ):
        """

        :param stream:
        :param plugins_manager:
        :return:
        """
        remote_address, remote_port = stream.get_peer_info()
        try:
            connect = await ConnectPacket.from_stream(stream)
        except NoDataException:
            raise MQTTException("Client closed the connection")  # pylint:disable=W0707
        logger.debug("< B %r", connect)
        await plugins_manager.fire_event(EVENT_MQTT_PACKET_RECEIVED, packet=connect)
        # this shouldn't be required anymore since broker generates for each client a random client_id if not provided
        # [MQTT-3.1.3-6]
        if connect.payload.client_id is None:
            raise MQTTException("[[MQTT-3.1.3-3]] : Client identifier must be present")

        if connect.payload.pk is None:
            raise MQTTException("[[MQTT-3.1.3-3]] : PK must be present on connection")

        if connect.variable_header.will_flag:
            if (
                connect.payload.will_topic is None
                or connect.payload.will_message is None
            ):
                raise MQTTException(
                    "will flag set, but will topic/message not present in payload"
                )

        if connect.variable_header.reserved_flag:
            raise MQTTException("[MQTT-3.1.2-3] CONNECT reserved flag must be set to 0")
        if connect.proto_name != "MQTT":
            raise MQTTException(
                '[MQTT-3.1.2-1] Incorrect protocol name: "%s"' % connect.proto_name
            )

        connack = None
        error_msg = None
        if connect.proto_level != 4:
            # only MQTT 3.1.1 supported
            error_msg = "Invalid protocol from %s: %d" % (
                format_client_message(address=remote_address, port=remote_port),
                connect.proto_level,
            )
            connack = ConnackPacket.build(
                0, UNACCEPTABLE_PROTOCOL_VERSION
            )  # [MQTT-3.2.2-4] session_parent=0
        elif not connect.username_flag and connect.password_flag:
            connack = ConnackPacket.build(0, BAD_USERNAME_PASSWORD)  # [MQTT-3.1.2-22]
        elif connect.username_flag and not connect.password_flag:
            connack = ConnackPacket.build(0, BAD_USERNAME_PASSWORD)  # [MQTT-3.1.2-22]
        elif connect.username_flag and connect.username is None:
            error_msg = "Invalid username from %s" % (
                format_client_message(address=remote_address, port=remote_port)
            )
            connack = ConnackPacket.build(
                0, BAD_USERNAME_PASSWORD
            )  # [MQTT-3.2.2-4] session_parent=0
        elif connect.password_flag and connect.password is None:
            error_msg = "Invalid password %s" % (
                format_client_message(address=remote_address, port=remote_port)
            )
            connack = ConnackPacket.build(
                0, BAD_USERNAME_PASSWORD
            )  # [MQTT-3.2.2-4] session_parent=0
        elif connect.clean_session_flag is False and (
            connect.payload.client_id_is_random
        ):
            error_msg = (
                "[MQTT-3.1.3-8] [MQTT-3.1.3-9] %s: No client Id provided (cleansession=0)"
                % (format_client_message(address=remote_address, port=remote_port))
            )
            connack = ConnackPacket.build(0, IDENTIFIER_REJECTED)
        if connack is not None:
            logger.debug("B > %r", connack)
            await plugins_manager.fire_event(EVENT_MQTT_PACKET_SENT, packet=connack)
            await connack.to_stream(stream)

            await stream.close()
            raise MQTTException(error_msg)

        incoming_session = Session(plugins_manager)
        incoming_session.client_id = connect.client_id
        incoming_session.pk = connect.payload.pk
        incoming_session.ecqv = ecqv
        incoming_session.capath = capath
        incoming_session.clean_session = connect.clean_session_flag
        incoming_session.will_flag = connect.will_flag
        incoming_session.will_retain = connect.will_retain_flag
        incoming_session.will_qos = connect.will_qos
        incoming_session.will_topic = connect.will_topic
        incoming_session.will_message = connect.will_message
        incoming_session.username = connect.username
        incoming_session.password = connect.password
        if connect.keep_alive > 0:
            incoming_session.keep_alive = connect.keep_alive
        else:
            incoming_session.keep_alive = 0

        handler = cls(plugins_manager)
        return handler, incoming_session
