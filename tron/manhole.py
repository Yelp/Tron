from twisted.conch.insults import insults
from twisted.conch.manhole import ColoredManhole
from twisted.conch.telnet import TelnetBootstrapProtocol
from twisted.conch.telnet import TelnetTransport
from twisted.internet import protocol


def make_manhole(namespace):
    f = protocol.ServerFactory()
    f.protocol = lambda: TelnetTransport(
        TelnetBootstrapProtocol,
        insults.ServerProtocol,
        ColoredManhole,
        namespace
    )
    return f
