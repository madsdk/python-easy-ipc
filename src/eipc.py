# An IPC handler that allows you to work with IPC through simple
# function calls instead of having to define some
# state machine/protocol.

from __future__ import with_statement
from threading import Thread
from thread import allocate_lock
from multiprocessing import Pipe, Process
from types import FunctionType, StringType, MethodType
from functools import partial

class EIPC(Thread):
    """The Extended/Easy IPC handler."""
    
    POLL_PERIOD = 0.1

    def __init__(self, pipe_in, pipe_out):
        """
        @type pipe_in: multiprocessing.Pipe
        @param pipe_in: The pipe used to receive data.
        @type pipe_out: multiprocessing.Pipe
        @param pipe_out: The pipe used to send data.
        """
        super(EIPC, self).__init__()
        self._pipe_in = pipe_in
        self._pipe_out = pipe_out
        self._pipe_out_lock = allocate_lock()
        self._shutdown = False
        self._functions = {}

    @classmethod
    def eipc_pair(cls):
        in_x, out_y = Pipe()
        out_x, in_y = Pipe()
        x = cls(in_x, out_x)
        y = cls(in_y, out_y)
        return (x, y)
    
    def register_function(self, function, name = ''):
        # Do simple type checking.
        if not type(function) in (FunctionType, MethodType) or type(name) != StringType: 
            raise TypeError('Arguments of invalid type given.')
        
        # Check that the name is not already taken.
        if name == '':
            name = function.__name__
        if self._functions.has_key(name):
            raise Exception('The function name %s is already taken.'%name)
        
        # Add the function to the list.
        self._functions[name] = function

    def stop(self, block = True):
        self._shutdown = True
        if block:
            self.join()

    def run(self):
        self._shutdown = False
        while not self._shutdown:
            if self._pipe_in.poll(EIPC.POLL_PERIOD):
                # Fetch the command.
                function_name, args, kwargs = self._pipe_in.recv()
                # Try to perform the function.
                try:
                    result = self.handle_remote_call(function_name, *args, **kwargs)
                except Exception, e:
                    # An error occurred. Return the error message.
                    self._pipe_in.send(('error', e.args[0]))
                    continue
                # Return the result.
                self._pipe_in.send(('ok', result))

    def handle_remote_call(self, function_name, *args, **kwargs):
        # Check that the function exists.
        if not self._functions.has_key(function_name):
            raise Exception('No such function registered.')

        # Call the function.
        return self._functions[function_name](*args, **kwargs)
        
    def call_remote_function(self, function_name, *args, **kwargs):
        with self._pipe_out_lock:
            # Send the command.
            self._pipe_out.send((function_name, args, kwargs))
            # Wait for the result to arrive.
            (rcode, result) = self._pipe_out.recv()
            # Act on the received data.
            if rcode == 'ok':
                return result
            elif rcode == 'error':
                raise Exception('Remote error -> %s'%result)
                
    def __nonzero__(self):
        return True

    def __getattr__(self, attrname):
        """
        Forwards any unknown attribute requests to the function that 
        makes remote calls.
        @type attrname: str
        @param attrname: The attribute to search for. In this case it must 
        be the name of a remote method. 
        """
        if attrname == '':
            return self
        else:
            return partial(self.call_remote_function, attrname)

class EIPCProcess(Process):
    """
    A process wrapper with built-in support for EIPC communication.
    This process will do all its work through IPC calls, i.e., its
    run method does nothing by itself, it merely serves the IPC.
    """

    def __init__(self, ipc):
        """
        Standard constructor.
        Arguments:
        - `ipc`: the EIPC pipe to communicate over.
        """
        super(EIPCProcess, self).__init__()
        self._ipc = ipc
        self.register_function(self.stop, 'remote_stop')
        
    def register_function(self, function, name=''):
        self._ipc.register_function(function, name)
        
    def stop(self):
        self._ipc.stop(False)

    def run(self):
        self._ipc.run()
