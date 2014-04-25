'''
RemoteBundleClient is a BundleClient implementation that shells out to a
BundleRPCServer for each command. Filesystem operations are implemented using
the FileServer operations exposed by the RPC server.
'''
import contextlib
import sys
import urllib
import xmlrpclib

from codalab.client.bundle_client import BundleClient
from codalab.common import UsageError
from codalab.lib import (
  file_util,
  zip_util,
)
from codalab.server.rpc_file_handle import RPCFileHandle

class AuthenticatedTransport(xmlrpclib.SafeTransport):
    '''
    Provides an implementation of xmlrpclib.Transport which injects an
    Authorization header into HTTP requests to the remove server.
    '''
    def __init__(self, address, get_auth_token):
        '''
        address: the address of the remote server
        get_auth_token: a function which yields the access token for
          the Bearer authentication scheme.
        '''
        xmlrpclib.SafeTransport.__init__(self, use_datetime=0)
        url_type, _ = urllib.splittype(address)
        if url_type not in ("http", "https"):
            raise IOError("unsupported XML-RPC protocol")
        self._url_type = url_type
        self._bearer_token = get_auth_token

    def send_content(self, connection, request_body):
        '''
        Overrides Transport.send_content in order to inject Authorization header.
        '''
        _, command = xmlrpclib.loads(request_body)
        token = self._bearer_token(command)
        if token is not None and len(token) > 0:
            connection.putheader("Authorization", "Bearer: {0}".format(token))
        xmlrpclib.SafeTransport.send_content(self, connection, request_body)

    def make_connection(self, host):
        '''
        Create a connection based on the communication scheme, http vs https.
        '''
        if self._url_type == "https":
            return xmlrpclib.SafeTransport.make_connection(self, host)
        else:
            return xmlrpclib.Transport.make_connection(self, host)

class RemoteBundleClient(BundleClient):
    CLIENT_COMMANDS = (
      'make',
      'run',
      'edit',
      'delete',
      'info',
      'ls',
      'search',
      # Worksheet-related commands all have JSON-able inputs and outputs.
      'new_worksheet',
      'list_worksheets',
      'worksheet_info',
      'add_worksheet_item',
      'update_worksheet',
      'rename_worksheet',
      'delete_worksheet',
    )
    COMMANDS = CLIENT_COMMANDS + (
      'open_target',
      'open_temp_file',
      'read_file',
      'close_file',
      'upload_zip',
      'login',
    )

    def __init__(self, address, get_auth_token):
        self.address = address
        transport = AuthenticatedTransport(address, get_auth_token)
        self.proxy = xmlrpclib.ServerProxy(address, transport=transport, allow_none=True)
        def do_command(command):
            def inner(*args, **kwargs):
                try:
                    return getattr(self.proxy, command)(*args, **kwargs)
                except xmlrpclib.ProtocolError, e:
                    if e.errcode == 401:
                        raise UsageError("Could not authenticate request.")
                    else:
                        raise
                except xmlrpclib.Fault, e:
                    # Transform server-side UsageErrors into client-side UsageErrors.
                    if 'codalab.common.UsageError' in e.faultString:
                        index = e.faultString.find(':')
                        raise UsageError(e.faultString[index + 1:])
                    else:
                        raise
            return inner
        for command in self.COMMANDS:
            setattr(self, command, do_command(command))

    def upload(self, bundle_type, path, metadata, worksheet_uuid=None):
        zip_path = zip_util.zip(path)
        with open(zip_path, 'rb') as source:
            remote_file_uuid = self.open_temp_file()
            dest = RPCFileHandle(remote_file_uuid, self.proxy)
            with contextlib.closing(dest):
                # FileServer does not expose an API for forcibly flushing writes, so
                # we rely on closing the file to flush it.
                file_util.copy(source, dest, autoflush=False)
        return self.upload_zip(bundle_type, remote_file_uuid, metadata, worksheet_uuid)

    def open_file(self, target):
        remote_file_uuid = self.open_target(target)
        return RPCFileHandle(remote_file_uuid, self.proxy)

    def tail_file(self, target):
        (bundle_spec, subpath) = target
        file_handle = self.open_file(target)

        with contextlib.closing(file_handle):
            # Print last 10 lines
            tail = file_handle.tail()
            print tail

            def read_line():
                return file_handle.readline()

            return self.watch(bundle_spec, [read_line])

    def tail_bundle(self, bundle_spec):
        out = self.open_file((bundle_spec, 'stdout'))
        err = self.open_file((bundle_spec, 'stderr'))

        with contextlib.closing(out), contextlib.closing(err):

            def out_line():
                return out.readline()
            def err_line():
                return err.readline()

            return self.watch(bundle_spec, [out_line, err_line])


    def cat(self, target):
        source = self.open_file(target)
        with contextlib.closing(source):
            file_util.copy(source, sys.stdout)
