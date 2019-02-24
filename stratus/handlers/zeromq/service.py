import json, string, random, abc, os, pickle, collections
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
from stratus.handlers.base import Handler
from stratus.handlers.client import StratusClient
from stratus.util.config import Config, StratusLogger
from .client import ZMQClient
from threading import Thread
import zmq, traceback, time, logging, xml, socket
from stratus_endpoint.handler.base import Task, Status
from typing import List, Dict, Sequence, Set
import random, string, os, queue, datetime
from stratus.util.parsing import s2b, b2s, ia2s, sa2s, m2s
import xarray as xa
from enum import Enum
MB = 1024 * 1024

class ServiceHandler( Handler ):

    def __init__(self, **kwargs ):
        htype = os.path.basename(os.path.dirname(__file__))
        super(ServiceHandler, self).__init__( htype, **kwargs )

    def newClient(self) -> StratusClient:
        return ZMQClient( **self.parms )

# class ExecutionCallback:
#   def success( results: xml.Node  ): pass
#   def failure( msg: str ): pass

class Response:

    def __init__(self, sid: str, body: Dict ):
        self._id = sid
        self._body = body

    @property
    def id(self): return self._id

    @property
    def message(self) -> str: return json.dumps(self._body)

    def __str__(self) -> str: return "[" + self.__class__.__name__  + "]: " + self.message

class DataPacket(Response):

    def __init__( self, sid: str, header: Dict, data: bytes = bytearray(0)  ):
        super(DataPacket, self).__init__( sid, header )
        self._data = data

    def hasData(self) -> bool:
        return ( self._data is not None ) and ( len( self._data ) > 0 )

    def getTransferHeader(self) -> bytes:
        return s2b( self.message )

    def getTransferData(self) -> bytes:
        return self._data

    def getRawData(self) -> bytes:
        return self._data

    def toString(self) -> str: return \
        "DataPacket[" + self.message + "]"

class Responder(Thread):

    def __init__( self,  _context: zmq.Context, _response_port: int, input_tasks: queue.Queue, **kwargs ):
        super(Responder, self).__init__()
        self.logger =  StratusLogger.getLogger()
        self.context: zmq.Context =  _context
        self.response_port = _response_port
        self.executing_jobs: Dict[str,Response] = {}
        self.status_reports: Dict[str,str] = {}
        self.client_address = kwargs.get( "client_address", "*" )
        self.socket: zmq.Socket = self.initSocket()
        self.input_tasks = input_tasks
        self.current_tasks: Dict[str,Task] = {}
        self.completed_tasks = collections.deque()
        self.pause_duration = 0.1
        self.active = True

    def getDataPacket(self, status: Status, task: Task ):
        if (status == Status.COMPLETED):
            return self.createDataPacket(task.id, task.getResult())
        elif (status == Status.ERROR):
            return self.createMessage(task.id, {"error": task["error"]})

    def importTasks(self):
        while not self.input_tasks.empty():
            task = self.input_tasks.get()
            self.current_tasks[task.id] = task

    def removeCompletedTasks(self):
        for completed_task in self.completed_tasks:
            del self.current_tasks[completed_task]
        self.completed_tasks.clear()

    def processResults(self):
        self.importTasks()
        for tid, task in self.current_tasks.items():
            status = task.status
            self.setExeStatus( tid, status )
            if status in [Status.COMPLETED, Status.ERROR]:
                dataPacket = self.getDataPacket( status, task )
                self.sendDataPacket( dataPacket )
                self.completed_tasks.append(tid)
        self.removeCompletedTasks()

    def run(self):
        while self.active:
            self.processResults()
            time.sleep( self.pause_duration )

    def sendDataPacket( self, dataPacket: DataPacket ):
        multipart_msg = [ s2b( dataPacket.id ), dataPacket.getTransferHeader() ]
        if dataPacket.hasData():
            bdata: bytes = dataPacket.getTransferData()
            multipart_msg.append( bdata )
            self.logger.info("@@R: Sent data packet for " + dataPacket.id + ", data Size: " + str(len(bdata)) )
            self.logger.info("@@R: Data header: " + dataPacket.message)
        else:
            self.logger.info( "@@R: Sent data header only for " + dataPacket.id + "---> NO DATA!" )
        self.socket.send_multipart( multipart_msg )

    def setExeStatus( self, sid: str, status: Status ):
        self.status_reports[sid] = status
        try:
            if status == Status.EXECUTING:
                self.executing_jobs[sid] = Response( sid, { "status": "executing" } )
            elif  status == Status.ERROR or status == Status.COMPLETED:
                del self.executing_jobs[sid]
        except Exception: pass

    def initSocket(self) -> zmq.Socket:
        socket: zmq.Socket   = self.context.socket(zmq.PUB)
        try:
            socket.bind( "tcp://{}:{}".format( self.client_address, self.response_port ) )
            self.logger.info( "@@R: --> Bound response socket to client at {} on port: {}".format( self.client_address, self.response_port ) )
        except Exception as err:
            self.logger.error( "@@R: Error initializing response socket on {}, port {}: {}".format( self.client_address, self.response_port, err ) )
            self.logger.error(traceback.format_exc())
        return socket

    def shutdown(self):
        self.active = False
        self.close_connection()

    def close_connection( self ):
        try:
            for response in self.executing_jobs.values():
                self.sendErrorMessage( f"Job {response.id} terminated  by server shutdown.")
            self.socket.close()
        except Exception: pass

    def sendMessage(self, sid: str, message: Dict = None):
        dataPacket = self.createMessage( sid, message )
        self.sendDataPacket(dataPacket)

    def sendErrorMessage(self, sid: str, message: str = None):
        self.sendMessage(sid, { "error": message }  )

    def createDataPacket( self, sid: str, dataset: xa.Dataset, metadata: Dict = None ) -> DataPacket:
        data = pickle.dumps(dataset, protocol=-1)
        header = metadata if metadata else {}
        header["type"] = "xarray"
        return DataPacket( sid, header, data )

    def createMessage(self, sid: str, message: Dict = None ) -> DataPacket:
        if "type" not in message: message["type"] = "message"
        return DataPacket( sid, message )

    def __del__(self):
        self.shutdown()