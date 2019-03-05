import json, string, random, abc, os
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple
from stratus.handlers.base import Handler
from stratus.handlers.client import StratusClient
from stratus.handlers.app import StratusCore, StratusAppBase
from .client import DirectClient

class ServiceHandler( Handler ):

    def __init__(self, **kwargs ):
        htype = os.path.basename(os.path.dirname(__file__))
        super(ServiceHandler, self).__init__( htype, **kwargs )

    def newClient(self) -> StratusClient:
        return DirectClient( **self.parms )

    def newApplication(self, core: StratusCore ) -> StratusAppBase:
        raise Exception( "Can't stand up a stratus app for an endpoint")






