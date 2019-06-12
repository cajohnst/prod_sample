import collections 
import threading 

# import for use as decorator 
from src.ib_utils.ibapi.utils import iswrapper
from src.ib_utils.ibapi import wrapper
from src.ib_utils.ibapi import utils
from src.ib_utils.ibapi.client import EClient
from src.ib_utils.ibapi.utils import iswrapper

# import all types
from src.ib_utils.ibapi.common import * # @UnusedWildImport
from src.ib_utils.ibapi.order_condition import * # @UnusedWildImport
from src.ib_utils.ibapi.contract import * # @UnusedWildImport
from src.ib_utils.ibapi.order import * # @UnusedWildImport
from src.ib_utils.ibapi.order_state import * # @UnusedWildImport
from src.ib_utils.ibapi.execution import Execution
from src.ib_utils.ibapi.execution import ExecutionFilter
from src.ib_utils.ibapi.commission_report import CommissionReport
from src.ib_utils.ibapi.ticktype import * # @UnusedWildImport
from src.ib_utils.ibapi.tag_value import TagValue

# import acct summary tags 
from src.ib_utils.ibapi.account_summary_tags import *

# import Client and Wrapper 
from src.ib_utils.utils.ib_api_client_socket import IBAPIClientSocket
from src.ib_utils.utils.ib_api_wrapper_interface import IBAPIWrapperInterface

def wrapper_method(fn):
    def result(*args):
        fn(*args)
        log_wrapper_fn = getattr(super(args[0].__class__, args[0]), fn.__name__)
        log_wrapper_fn(*args[1:])
        args[0].sender(fn.__name__, *args[1:])
    return result

class IBAPI(IBAPIWrapperInterface, IBAPIClientSocket): 
    """
    DESCRIPTION
    -----------
    The IBAPI class handles sending and receiving messages from the IB API client
    socket.  We will establish all handler functions in this class which can be 
    called from higher method classes, for instance the Trading class or the HistoricalData
    class.  This class has two parent inheritances, IBAPIWrapperInterface and 
    IBAPIClientSocket, both which reference higher inheritances themselves, the wrapper.EWrapper
    and EClient classes respectively.  
    
    This class will be used as a base level API connection to be used by higher 
    level handlers, ultimately to be managed by the API manager.  This is the 
    current design.

    PARAMETERS
    ----------
    host: str (default="127.0.0.1")
        A string representation of the host ip address, the default is localhost.
    port : int (default=7497)
        An integer representing the port number for the websocket listening.
        The default IB websocket was 7496 but has since migrated to 7497.
    client_id : int (default=0)
        An integer value representing the client id number used to make requests
        across the API.  The default 0 value is the master client id, which is 
        capable of all read, write, and execute privileges.

    MODIFICATIONS
    -------------
    Created : 6/12/19
    """
    def __init__(self, host="127.0.0.1", port=7497, client_id=0):
        IBAPIWrapperInterface.__init__(self)
        IBAPIClientSocket.__init__(self, wrapper=self)
        self.host = host
        self.port = port 
        self.client_id = client_id 
        self.nKeybInt = 0
        self.started = False
        self.nextValidOrderId = None
        self.permId2ord = {}
        self.reqId2nErr = collections.defaultdict(int)
        self.receivers = {}

    def sender(self, meth_name, *args):
        """
        DESCRIPTION
        -----------

        PARAMETERS
        ----------
        meth_name : attr
            The name of a class method instance which to call.
        args : args
            Passing the arguments of the method 
        """
        if meth_name in self.receivers:
            for receiver in self.receivers[meth_name]:
                try:
                    receiver(*args)
                except Exception as e:
                    self.log.exception(e)
                
    def register(self, receiver, msg_type):
        """
        DESCRIPTION
        -----------
        Register a receiver function to handle a response from IB of a certain 
        message type which is of interest.

        PARAMETERS
        ----------
        receiver : func
            A function which handles an incoming message.
        msg_type : str
            A message type referenced at the end of an incoming message.  See 
            ib_api_calls in utils as a reference guide.

        MODIFICATIONS
        -------------
        Created : 6/12/19
        """
        receivers = self.receivers.setdefault(msg_type, [])
        if receiver not in receivers:
            receivers.append(receiver)
            
    def unregister(self, receiver, msg_type):
        """
        DESCRIPTION
        -----------
        Unregister a receiver function handling a particular message type to 
        cease listening for messages of this type from IB.

        receiver : func
            A funcion which handles an incoming message.
        msg_type : str 
            A message type referenced at the end of an incoming message.  See 
            ib_api_calls in utils as a reference guide.

        MODIFICATIONS
        -------------
        Created : 6/12/19
        """
        try:
            receivers = self.receivers[msg_type]
        except (KeyError, ):
            pass
        else:
            if receiver in receivers:
                receivers.remove(receiver)

    @wrapper_method
    def sample_handler(self, reqId):
        pass 


