# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import logging
import attr
import anyio
import yaml
import os

from .errors import InvalidStateError

logger = logging.getLogger(__name__)


def format_client_message(session=None, address=None, port=None):
    if session:
        return "(client id=%s)" % session.client_id
    elif address is not None and port is not None:
        return "(client @=%s:%d)" % (address, port)
    else:
        return "(unknown client)"


def gen_client_id():
    """
    Generates random client ID
    :return:
    """
    import random

    gen_id = "distmqtt-"

    for _ in range(16):
        gen_id += chr(random.randint(0, 26) + 97)
    return gen_id


def read_yaml_config(config_file):
    config = None
    try:
        with open(config_file, "r") as stream:
            config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        logger.error("Invalid config_file %s: %r", config_file, exc)
    return config


def ecqv_pk_extract(ecqv_utils_path, priv):
    s = os.popen('%s pk_extract -k "%s"' % (ecqv_utils_path, priv))
    return s.read().strip()


def ecqv_priv_extract(ecqv_utils_path, priv):
    s = os.popen('%s priv_extract -k "%s"' % (ecqv_utils_path, priv))
    return s.read().strip()


def ecqv_cert_request(ecqv_utils_path, identity, key_path):
    s = os.popen(
        '%s cert_request -i "%s" -k "%s"' % (ecqv_utils_path, identity, key_path)
    )
    return s.read().strip()


def ecqv_cert_generate(ecqv_utils_path, identity, requester_pk, key_path):
    s = os.popen(
        '%s cert_generate -i "%s" -r "%s" -k "%s"'
        % (ecqv_utils_path, identity, requester_pk, key_path)
    )
    return s.read().strip().split("\n")


def ecqv_cert_reception(ecqv_utils_path, identity, key_path, ca_pk, cert, r):
    s = os.popen(
        '%s cert_reception -i "%s" -k "%s" -c "%s" -a "%s" -r "%s"'
        % (
            ecqv_utils_path,
            identity,
            key_path,
            ca_pk,
            cert,
            r,
        )
    )
    return s.read().strip()


def ecqv_generate_confirmation(ecqv_utils_path, ca_pk, cert_priv_key, g_path):
    s = os.popen(
        '%s generate_confirmation -c "%s" -d "%s" -g "%s"'
        % (
            ecqv_utils_path,
            ca_pk,
            cert_priv_key,
            g_path,
        )
    )
    return "".join(s.read().strip().split())


def ecqv_verify_confirmation(ecqv_utils_path, verify, cert_pk, g_pk, ca_path):
    s = os.popen(
        '%s verify_confirmation -v "%s" -d "%s" -g "%s" -k "%s"'
        % (
            ecqv_utils_path,
            verify,
            cert_pk,
            g_pk,
            ca_path,
        )
    )
    response = s.read().strip()
    if len(response):
        return response
    return None


def ecqv_group_generate(ecqv_utils_path, ca_path, ids, g_pks, cert_pks, verify_numbers):
    s = os.popen(
        '%s group_generate -c "%s" -i "%s" -g "%s" -d "%s" -v "%s"'
        % (
            ecqv_utils_path,
            ca_path,
            ",".join(ids),
            ",".join(g_pks),
            ",".join(cert_pks),
            ",".join(verify_numbers),
        )
    )
    response = s.read().strip().split()
    return tuple(response)


def ecqv_cert_pk_extract(ecqv_utils_path, identity, ca_pk, cert):
    s = os.popen(
        '%s cert_pk_extract -i "%s" -c "%s" -a "%s"'
        % (
            ecqv_utils_path,
            identity,
            ca_pk,
            cert,
        )
    )
    return s.read().strip()


def ecqv_encrypt(ecqv_utils_path, key, message):
    s = os.popen('%s encrypt -k "%s" -m "%s"' % (ecqv_utils_path, key, message))

    return s.read().strip()


def ecqv_decrypt(ecqv_utils_path, key, cypher):
    s = os.popen('%s decrypt -k "%s" -m "%s"' % (ecqv_utils_path, key, cypher))

    return s.read().strip()


def ecqv_mul(ecqv_utils_path, priv, pub):
    s = os.popen('%s mul -k "%s" -p "%s"' % (ecqv_utils_path, priv, pub))
    return s.read().strip()


def ecqv_sign(ecqv_utils_path, key, message):
    print('%s sign -k "%s" -m "%s"' % (ecqv_utils_path, key, message))
    s = os.popen('%s sign -k "%s" -m "%s"' % (ecqv_utils_path, key, message))

    return s.read().strip().split("\n")


def ecqv_verify(ecqv_utils_path, key, message, v, signature):
    print(
        '%s verify -k "%s" -m "%s" -v "%s" -s "%s"'
        % (ecqv_utils_path, key, message, v, signature)
    )
    s = os.popen(
        '%s verify -k "%s" -m "%s" -v "%s" -s "%s"'
        % (ecqv_utils_path, key, message, v, signature)
    )

    return s.read().strip()


# utility code


class CancelledError(RuntimeError):
    # This intentionally does not descend from any toolkit's cancellation exception
    # (much less from all of them)
    pass


@attr.s
class Future:
    """A waitable value useful for inter-task synchronization,
    inspired by :class:`threading.Event` and :class:`asyncio.Future`.

    An event object manages an internal value, which is initially
    unset, and a task can wait for it to become True.

    Note that the value can only be read once.
    """

    event = attr.ib(factory=anyio.Event, init=False)
    value = attr.ib(default=None, init=False)

    async def set(self, value):
        """Set the result to return this value, and wake any waiting task."""
        if self.event.is_set():
            raise InvalidStateError("Value already set")
        self.value = value
        self.event.set()

    async def set_error(self, exc):
        """Set the result to raise this exceptio, and wake any waiting task."""
        if self.event.is_set():
            raise InvalidStateError("Value already set")
        self.value = exc
        self.event.set()

    def is_set(self):
        """Check whether the event has occurred."""
        return self.value is not None

    async def cancel(self):
        """Send a cancelation to the recipient."""
        await self.set_error(CancelledError())

    async def get(self):
        """Block until the value is set.

        If it's already set, then this method returns immediately.

        The value can only be read once.
        """
        await self.event.wait()
        if isinstance(self.value, BaseException):
            raise self.value
        return self.value

    def done(self):
        """Report whether this Future has been set."""
        return self.event.is_set()


def match_topic(topic, subscription):
    """
    Match @topic to @subscription. Both must be lists/tuples.
    """
    if isinstance(topic, str) or isinstance(subscription, str):
        raise RuntimeError("Subscriptions need to be pre-split")
    if topic[0].startswith("$") != subscription[0].startswith("$"):
        return False
    if len(topic) < len(subscription):
        return False
    if len(topic) > len(subscription) and subscription[-1] != "#":
        return False
    for a, b in zip(topic, subscription):
        if a != b and b not in ("+", "#"):
            return False
    return True


try:
    from anyio import create_queue
except ImportError:
    from anyio import create_memory_object_stream as _cmos

    class Queue:
        def __init__(self, length=0):
            self._s, self._r = _cmos(length)

        def put(self, x):
            return self._s.send(x)

        def get(self):
            return self._r.receive()

        def qsize(self):
            return len(self._s._state.buffer)  # ugh

        def empty(self):
            return not len(self._s._state.buffer)  # ugh

        def __aiter__(self):
            return self

        def __anext__(self):
            return self._r.__anext__()

        def close_sender(self):
            return self._s.aclose()

        def close_receiver(self):
            return self._r.aclose()

    def create_queue(length=0):
        return Queue(length)
