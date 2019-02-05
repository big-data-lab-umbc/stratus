import string, random, abc, os, yaml, json
from typing import List, Dict, Any, Sequence, Callable, BinaryIO, TextIO, ValuesView, Optional
from stratus.handlers.client import StratusClient

import abc, sys, pkgutil

class Handler:
    __metaclass__ = abc.ABCMeta

    def __init__(self, htype: str, **kwargs):
        self.parms = kwargs
        self.name = self['name']
        self.type: str = htype
        self._client = None
        htype1 = self.parms.pop("type")
        assert htype1 == htype, "Sanity check of Handler type failed: {} vs {}".format(htype1,htype)

    def __getitem__( self, key: str ) -> str:
        result =  self.parms.get( key, None )
        assert result is not None, "Missing required parameter in {}: {} ".format( self.__class__.__name__, key )
        return result

    def parm(self, key: str, default: str ) -> str:
        return self.parms.get( key, default  )

    @abc.abstractmethod
    def newClient(self) -> StratusClient: pass

    @property
    def client(self) -> StratusClient:
        if self._client is None:
            self._client = self.newClient()
            self._client.init()
        return self._client

    def __repr__(self):
        return json.dumps( self.parms )


# class Handler:
#     __metaclass__ = abc.ABCMeta
#
#     @classmethod
#     def randomStr(cls, length) -> str:
#         tokens = string.ascii_uppercase + string.ascii_lowercase + string.digits
#         return ''.join(random.SystemRandom().choice(tokens) for _ in range(length))
#
#     @abc.abstractmethod
#     def handles(self, op: str )-> bool : pass
#
#     @abc.abstractmethod
#     def processRequest(self, op: str, **kwargs ): pass
#
#
# class DebugHandler(Handler):
#
#     def handles(self, op: str )-> bool : return True
#
#     def processRequest(self, op: str, **kwargs ):
#         rid = kwargs.get("id", self.randomStr(8) )
#         kwargs.update( { "op" : op, "id": rid, "status": "complete" } )
#         return kwargs
#
# class Handlers:
#     handlers = [ DebugHandler() ]
#
#     @classmethod
#     def processRequest(cls, op, **kwargs ):
#         handler = cls.getHandler( op )
#         return handler.processRequest( op, **kwargs )
#
#     @classmethod
#     def getHandler( cls, op: str ):
#         for handler in cls.handlers:
#             if handler.handles( op ):
#                 return handler
#         return None
#
#     @classmethod
#     def addHandler(cls, handler ):
#         cls.handlers.insert( 0, handler )

if __name__ == "__main__":
    mgr = Handlers()
    print( str(mgr) )