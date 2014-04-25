'''
Abstract base class that describes the client interface for interacting with
the CodaLab bundle system.

There are three categories of BundleClient commands:
  - Commands that create and edit bundles: upload, make, run and update.
  - Commands for browsing bundles: info, ls, cat, search, and wait.
  - Various utility commands for pulling bundles back out of the system.

There are a couple of implementations of this class:
  - LocalBundleClient - interacts directly with a BundleStore and BundleModel.
  - RemoteBundleClient - shells out to a BundleRPCServer to implement its API.
'''
# TODO: We should probably implement grep at some point. grep will take a
# target (like the target passed to ls or cat) and a list of command-line args.
# The RemoteBundleClient implementation of grep will have to use the FileServer
# file-handle API to stream the results back.
import time
from sys import stdout

from codalab.common import State


class BundleClient(object):
    # Commands for creating/editing bundles: upload, make, run, edit, and delete.

    def upload(self, bundle_type, path, metadata, worksheet_uuid=None):
        '''
        Create a new bundle with a copy of the directory at the given path in the
        local filesystem. Return its uuid. If the path leads to a file, the new
        bundle will only contain only that file.
        '''
        raise NotImplementedError

    def make(self, targets, metadata):
        '''
        Create a new bundle with dependencies on the given targets. Return its uuid.
        targets should be a dict mapping target keys to (bundle_spec, path) pairs.
        Each of the targets will by symlinked into the new bundle at its key.
        '''
        raise NotImplementedError

    def run(self, program_target, input_target, command, metadata):
        '''
        Run the given program bundle, create bundle of output, and return its uuid.
        The program and input targets are (bundle_spec, path) pairs that are
        symlinked in as dependencies during runtime.
        '''
        raise NotImplementedError

    def edit(self, uuid, metadata):
        '''
        Update the bundle with the given uuid with the new metadata.
        '''
        raise NotImplementedError

    def delete(self, bundle_spec, force=False):
        '''
        bundle_spec should be either a bundle uuid, a unique prefix of a uuid, or
        a unique bundle name.

        Delete this bundle. If force is True, delete all its (direct and indirect)
        descendents too. If force is False, and if this bundle has any downstream
        dependencies, this method will raise a UsageError.
        '''
        raise NotImplementedError

    # Commands for browsing bundles: info, ls, cat, search, and wait.

    def info(self, bundle_spec, parents=False, children=False):
        '''
        Return a dict containing detailed information about a given bundle:
          bundle_type: one of (program, dataset, macro, make, run)
          data_hash: hash of the bundle's data, if the bundle is READY
          metadata: its metadata dict
          state: its current state
          uuid: its uuid
          hard_dependencies: list of this bundle's realized dependencies

        If parents is True, this dict will also map 'parents' to a list of string
        representations of each bundle that this bundle depends on. If children is
        True, then it will map 'children' to bundles that depend on this bundle.

        Note that the list of parents and children include all logical dependencies,
        not just dependencies that are realized within the final bundle. For
        example, a run would depend on its program and input even though symlinks to
        those bundles are deleted before the program is uploaded.
        '''
        raise NotImplementedError

    def ls(self, target):
        '''
        Return (list of directories, list of files) located underneath the target.
        The target should be a (bundle_spec, path) pair.
        '''
        raise NotImplementedError

    def cat(self, target):
        '''
        Print the contents of the target file at to stdout.
        '''
        raise NotImplementedError

    def tail_file(self, target):
        '''
        Watch the tail of target file at stdout.
        '''
        raise NotImplementedError

    def tail_bundle(self, bundle_spec):
        '''
        Watch the tail of stdout and stderr at stdout.
        '''
        raise NotImplementedError

    def search(self, query=None):
        '''
        Run a search on bundle metadata and return data for all bundles that match.
        The data for each bundle is a dict with the same keys as a dict from info.
        '''
        raise NotImplementedError

    def watch(self, bundle_spec, fns):
        '''
        Block on the execution of the given bundle.
        fns should be a list of functions that return strings.
        Periodically execute fns and print output.
        Return READY or FAILED based on whether it was computed successfully.
        '''
        # Constants for a simple exponential backoff routine that will decrease the
        # frequency at which we check this bundle's state from 1s to 1m.
        period = 1.0
        backoff = 1.1
        max_period = 60.0
        info = self.info(bundle_spec)
        while info['state'] not in (State.READY, State.FAILED):
            # Update bundle info
            info = self.info(bundle_spec)

            # Call update functions
            change = False
            for fn in fns:
                result = fn()
                if not result == '':
                    stdout.write(result)
                    stdout.flush()
                    change = True
            # Sleep if nothing happened
            if change == False:
                time.sleep(period)
                period = min(backoff*period, max_period)

        return info['state']

    def download(self, target):
        '''
        Download a bundle file over HTTP. Return PROGRESS or FAILED
        based on whether it was downloaded successfully.
        '''
        raise NotImplementedError

    def get_home(self):
        raise NotImplementedError

    def get_host(self):
        raise NotImplementedError

    def update_host(self):
        raise NotImplementedError

    def update_verbosity(self):
        raise NotImplementedError



    #############################################################################
    # Worksheet-related client methods follow!
    #############################################################################

    def new_worksheet(self, name):
        '''
        Create a new worksheet with the given name and return its uuid.
        '''
        raise NotImplementedError

    def list_worksheets(self):
        '''
        Return a list of worksheet row dicts. Does NOT include worksheet items.
        '''
        return self.model.list_worksheets()

    def worksheet_info(self, worksheet_spec):
        '''
        worksheet_spec should be either a worksheet uuid, a unique prefix of a uuid,
        or a unique worksheet name. Return an info dict for this worksheet.

        This dict will have the following keys:
          uuid: the worksheet uuid
          name: the worksheet name
          items: an list of (bundle_info, value) pairs, where bundle_info is either:
                  - a bundle info dict
                  - a dict mapping 'uuid' to a bundle_uuid, if the uuid is orphaned
                  - None (for non-bundle rows)
          last_item_id: the last database id of any item in the list
        '''
        raise NotImplementedError

    def add_worksheet_item(self, worksheet_spec, bundle_spec):
        '''
        Add the bundle specified by the bundle_spec to the worksheet specified by
        the worksheet_spec.
        '''
        raise NotImplementedError

    def update_worksheet(self, worksheet_info, new_items):
        '''
        Take a worksheet info dict and a list of new (bundle_spec, value) pairs and
        update the worksheet. Raise a UsageError if there was a concurrent update.
        '''
        raise NotImplementedError

    def rename_worksheet(self, worksheet_spec, name):
        '''
        Update the specified worksheet to have the new name.
        '''
        raise NotImplementedError

    def delete_worksheet(self, worksheet_spec):
        '''
        Delete the specified worksheet.
        '''
        raise NotImplementedError
