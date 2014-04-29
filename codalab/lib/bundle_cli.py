'''
BundleCLI is a class that provides one major API method, do_command, which takes
a list of CodaLab bundle system command-line arguments and executes them.

Each of the supported commands corresponds to a method on this class.
This function takes an argument list and an ArgumentParser and does the action.

  ex: BundleCLI.do_command(['upload', 'program', '.'])
   -> BundleCLI.do_upload_command(['program', '.'], parser)
'''
import argparse
import collections
import datetime
import itertools
import os
import re
import sys
import time

from codalab.bundles import (
  get_bundle_subclass,
  UPLOADED_TYPES,
)
from codalab.bundles.make_bundle import MakeBundle
from codalab.bundles.uploaded_bundle import UploadedBundle
from codalab.bundles.run_bundle import RunBundle
from codalab.common import (
  precondition,
  State,
  UsageError,
)
from codalab.lib import (
  metadata_util,
  path_util,
  spec_util,
  worksheet_util,
)

from codalab.objects.worker import Worker

class BundleCLI(object):
    DESCRIPTIONS = {
      'help': 'Show a usage message for cl or for a particular command.',
      'status': 'Show current status.',
      'upload': 'Create a bundle by uploading an existing file/directory.',
      'make': 'Create a bundle out of existing bundles.',
      'run': 'Create a bundle by running a program bundle on an input bundle.',
      'edit': "Edit an existing bundle's metadata.",
      'rm': 'Delete a bundle and all bundles that depend on it.',
      'list': 'Show basic information for all bundles [in a worksheet].',
      'info': 'Show detailed information for a single bundle.',
      'ls': 'List the contents of a bundle.',
      'cat': 'Print the contents of a file in a bundle.',
      'tail': 'Watch the contents of a file in a bundle.',
      'wait': 'Wait until a bundle is ready or failed, then print its state.',
      'kill': 'Instruct the worker to terminate a running bundle.',
      'download': 'Download remote bundle from URL.',
      'cp': 'Copy bundles across servers.',
      # Worksheet-related commands.
      'new': 'Create a new worksheet and make it the current one.',
      'add': 'Append a bundle to a worksheet.',
      'work': 'Set the current address/worksheet.',
      'print': 'Print the full-text contents of a worksheet.',
      'edit_worksheet': 'Rename a worksheet or open a full-text editor to edit it.',
      'list_worksheet': 'Show basic information for all worksheets.',
      'rm_worksheet': 'Delete a worksheet. Must specify a worksheet spec.',
      # Commands that can only be executed on a LocalBundleClient.
      'cleanup': 'Clean up the CodaLab bundle store.',
      'worker': 'Run the CodaLab bundle worker.',
      'reset': 'Delete the CodaLab bundle store and reset the database.',
      'server': 'Start an instance of the CodaLab server.',  # Note: this is not actually handled in BundleCLI
    }
    BUNDLE_COMMANDS = (
      'upload',
      'make',
      'run',
      'edit',
      'rm',
      'list',
      'info',
      'ls',
      'cat',
      'tail',
      'wait',
      'download',
      'cp',
      'server',
    )
    WORKSHEET_COMMANDS = (
      'new',
      'add',
      'work',
      'print',
    )
    # A list of commands for bundles that apply to worksheets with the -w flag.
    BOTH_COMMANDS = (
      'edit',
      'list',
      'rm',
      'cp',
    )

    def __init__(self, manager):
        self.manager = manager
        self.verbose = manager.config['cli']['verbose']

    def exit(self, message, error_code=1):
        '''
        Print the message to stderr and exit with the given error code.
        '''
        precondition(error_code, 'exit called with error_code == 0')
        print >> sys.stderr, message
        sys.exit(error_code)

    def hack_formatter(self, parser):
        '''
        Screw with the argparse default formatter to improve help formatting.
        '''
        formatter_class = parser.formatter_class
        if type(formatter_class) == type:
            def mock_formatter_class(*args, **kwargs):
                return formatter_class(max_help_position=30, *args, **kwargs)
            parser.formatter_class = mock_formatter_class

    def get_distinct_bundles(self, worksheet_info):
        '''
        Return list of info dicts of distinct, non-orphaned bundles in the worksheet.
        '''
        uuids_seen = set()
        result = []
        for (bundle_info, _) in worksheet_info['items']:
            if bundle_info and 'bundle_type' in bundle_info:
                if bundle_info['uuid'] not in uuids_seen:
                    uuids_seen.add(bundle_info['uuid'])
                    result.append(bundle_info)
        return result

    def parse_target(self, target, canonicalize=True):
        result = tuple(target.split(os.sep, 1)) if os.sep in target else (target, '')
        if canonicalize:
            # If canonicalize is True, we should immediately invoke the bundle client
            # to fully qualify the target's bundle_spec into a uuid.
            (bundle_spec, path) = result
            client = self.manager.current_client()
            info = client.info(bundle_spec)
            return (info['uuid'], path)
        return result

    def parse_key_targets(self, items):
        '''
        Items is a list of strings which are [<key>:]<target>
        '''
        targets = {}
        # Turn targets into a dict mapping key -> (uuid, subpath)) tuples.
        for item in items:
            if ':' in item:
                (key, target) = item.split(':', 1)
                if key == '': key = target  # Set default key
            else:
                # Provide syntactic sugar for a make bundle with a single anonymous target.
                (key, target) = ('', item)
            if key in targets:
                if key:
                    raise UsageError('Duplicate key: %s' % (key,))
                else:
                    raise UsageError('Must specify keys when packaging multiple targets!')
            targets[key] = self.parse_target(target, canonicalize=True)
        return targets

    def print_table(self, columns, row_dicts):
        '''
        Pretty-print a list of columns from each row in the given list of dicts.
        '''
        rows = list(itertools.chain([columns], (
          [row_dict.get(col, '') for col in columns] for row_dict in row_dicts
        )))
        lengths = [max(len(value) for value in col) for col in zip(*rows)]
        for (i, row) in enumerate(rows):
            row_strs = []
            for (value, length) in zip(row, lengths):
                row_strs.append(value + (length - len(value))*' ')
            print '  '.join(row_strs)
            if i == 0:
                print (sum(lengths) + 2*(len(columns) - 1))*'-'

    def size_str(self, size):
        for unit in ('bytes', 'KB', 'MB', 'GB'):
            if size < 1024:
                return '%d %s' % (size, unit)
            size /= 1024

    def time_str(self, ts):
        return datetime.datetime.utcfromtimestamp(ts).isoformat().replace('T', ' ')

    GLOBAL_SPEC_FORMAT = "[<alias>::|<address>::]|[<uuid>|<name>]"
    TARGET_FORMAT = '[<key>:][<uuid>|<name>][%s<subpath within bundle>]' % (os.sep,)

    # Example: http://codalab.org::wine
    # Return (client, spec)
    def parse_spec(self, spec):
        tokens = spec.split('::')
        if len(tokens) == 1:
            address = self.manager.session()['address']
            spec = tokens[0]
        else:
            address = self.manager.apply_alias(tokens[0])
            spec = tokens[1]
        return (self.manager.client(address), spec)

    def parse_client_worksheet_info(self, spec):
        client, spec = self.parse_spec(spec)
        return (client, client.worksheet_info(spec))
        
    def parse_client_bundle_info_list(self, spec):
        client, spec = self.parse_spec(spec)
        return (client, client.info(spec))
        
    #############################################################################
    # CLI methods
    #############################################################################

    def do_command(self, argv):
        if argv:
            (command, remaining_args) = (argv[0], argv[1:])
            # Multiplex between `edit` and `edit -w` (which becomes edit_worksheet),
            # and likewise between other commands for both bundles and worksheets.
            if command in self.BOTH_COMMANDS and '-w' in remaining_args:
                remaining_args = [arg for arg in remaining_args if arg != '-w']
                command = command + '_worksheet'
        else:
            (command, remaining_args) = ('help', [])
        command_fn = getattr(self, 'do_%s_command' % (command,), None)
        if not command_fn:
            self.exit("'%s' is not a CodaLab command. Try 'cl help'." % (command,))
        parser = argparse.ArgumentParser(
          prog='cl %s' % (command,),
          description=self.DESCRIPTIONS[command],
        )
        self.hack_formatter(parser)
        if self.verbose:
            command_fn(remaining_args, parser)
        else:
            try:
                return command_fn(remaining_args, parser)
            except UsageError, e:
                self.exit('%s: %s' % (e.__class__.__name__, e))

    def do_help_command(self, argv, parser):
        if argv:
            self.do_command([argv[0], '-h'] + argv[1:])
        print 'usage: cl <command> <arguments>'
        max_length = max(
          len(command) for command in
          itertools.chain(self.BUNDLE_COMMANDS, self.WORKSHEET_COMMANDS)
        )
        indent = 2
        def print_command(command):
            print '%s%s%s%s' % (
              indent*' ',
              command,
              (indent + max_length - len(command))*' ',
              self.DESCRIPTIONS[command],
            )
        print '\nCommands for bundles:'
        for command in self.BUNDLE_COMMANDS:
            print_command(command)
        print '\nCommands for worksheets:'
        for command in self.WORKSHEET_COMMANDS:
            print_command(command)
        for command in self.BOTH_COMMANDS:
            print '  %s%sUse `cl %s -w` to %s worksheets.' % (
              command, (max_length + indent - len(command))*' ', command, command)

    def do_status_command(self, argv, parser):
        print "session: %s" % self.manager.session_name()
        print "address: %s" % self.manager.session()['address']
        worksheet_info = self.get_current_worksheet_info()
        if worksheet_info:
            print "worksheet: %s [%s]" % (worksheet_info['name'], worksheet_info['uuid'])

    def do_upload_command(self, argv, parser):
        client, worksheet_uuid = self.manager.get_current_worksheet_uuid()
        help_text = 'bundle_type: [%s]' % ('|'.join(sorted(UPLOADED_TYPES)))
        parser.add_argument('bundle_type', help=help_text)
        parser.add_argument('path', help='path of the directory to upload')
        # Add metadata arguments for UploadedBundle and all of its subclasses.
        metadata_keys = set()
        metadata_util.add_arguments(UploadedBundle, metadata_keys, parser)
        for bundle_type in UPLOADED_TYPES:
            bundle_subclass = get_bundle_subclass(bundle_type)
            metadata_util.add_arguments(bundle_subclass, metadata_keys, parser)
        metadata_util.add_auto_argument(parser)
        args = parser.parse_args(argv)
        # Check that the upload path exists.
        path_util.check_isvalid(path_util.normalize(args.path), 'upload')
        # Pull out the upload bundle type from the arguments and validate it.
        if args.bundle_type not in UPLOADED_TYPES:
            raise UsageError('Invalid bundle type %s (options: [%s])' % (
              args.bundle_type, '|'.join(sorted(UPLOADED_TYPES)),
            ))
        bundle_subclass = get_bundle_subclass(args.bundle_type)
        metadata = metadata_util.request_missing_data(bundle_subclass, args)
        # Type-check the bundle metadata BEFORE uploading the bundle data.
        # This optimization will avoid file copies on failed bundle creations.
        bundle_subclass.construct(data_hash='', metadata=metadata).validate()
        print client.upload(args.bundle_type, args.path, metadata, worksheet_uuid)

    def do_make_command(self, argv, parser):
        client, worksheet_uuid = self.manager.get_current_worksheet_uuid()
        parser.add_argument('target', help=self.TARGET_FORMAT, nargs='+')
        metadata_util.add_arguments(MakeBundle, set(), parser)
        metadata_util.add_auto_argument(parser)
        args = parser.parse_args(argv)
        targets = self.parse_key_targets(args.target)
        metadata = metadata_util.request_missing_data(MakeBundle, args)
        print client.make(targets, metadata, worksheet_uuid)

    def do_run_command(self, argv, parser):
        client, worksheet_uuid = self.manager.get_current_worksheet_uuid()
        parser.add_argument('target', help=self.TARGET_FORMAT, nargs='*')
        parser.add_argument('command', help='Command-line')
        metadata_util.add_arguments(RunBundle, set(), parser)
        metadata_util.add_auto_argument(parser)
        args = parser.parse_args(argv)
        targets = self.parse_key_targets(args.target)
        command = args.command
        if False:
            # Expand command = "@svmlight/run" => "svmlight:svmlight svmlight/run"
            pattern = re.compile(r'^([^@]*)@([^@]+)@(.*)$')
            while True:
                m = pattern.match(command)
                if not m: break
                before, key, after = m
                targets[key] = self.parse_target(key, canonicalize=True)  # key is the target
                command = before + key + after
        #print targets, command
        metadata = metadata_util.request_missing_data(RunBundle, args)
        print client.run(targets, command, metadata, worksheet_uuid)

    # DEPRECATED - remove
    def do_run_old_command(self, argv, parser):
        client, worksheet_uuid = self.manager.get_current_worksheet_uuid()
        parser.add_argument('program_target', help=self.TARGET_FORMAT)
        parser.add_argument('input_target', help=self.TARGET_FORMAT)
        parser.add_argument(
          'command',
          help='shell command with access to program, input, and output',
        )
        metadata_util.add_arguments(RunBundle, set(), parser)
        metadata_util.add_auto_argument(parser)
        args = parser.parse_args(argv)
        program_target = self.parse_target(args.program_target, canonicalize=True)
        input_target = self.parse_target(args.input_target, canonicalize=True)
        metadata = metadata_util.request_missing_data(RunBundle, args)
        print client.run(program_target, input_target, args.command, metadata, worksheet_uuid)

    def do_edit_command(self, argv, parser):
        parser.add_argument('bundle_spec', help='identifier: [<uuid>|<name>]')
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        info = client.info(args.bundle_spec)
        bundle_subclass = get_bundle_subclass(info['bundle_type'])
        new_metadata = metadata_util.request_missing_data(
          bundle_subclass,
          args,
          info['metadata'],
        )
        if new_metadata != info['metadata']:
            client.edit(info['uuid'], new_metadata)

    def do_rm_command(self, argv, parser):
        parser.add_argument('bundle_spec', help='identifier: [<uuid>|<name>]')
        parser.add_argument(
          '-f', '--force',
          action='store_true',
          help='delete all downstream dependencies',
        )
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        client.delete(args.bundle_spec, args.force)

    def do_list_command(self, argv, parser):
        parser.add_argument(
          '-a', '--all',
          action='store_true',
          help='list all bundles, not just this worksheet',
        )
        parser.add_argument(
          'worksheet_spec',
          help='identifier: %s (default: current worksheet)' % self.GLOBAL_SPEC_FORMAT,
          nargs='?',
        )
        args = parser.parse_args(argv)
        if args.all and args.worksheet_spec:
            raise UsageError("Can't use both --all and a worksheet spec!")
        source = ''
        if args.all:
            client = self.manager.current_client()
            bundle_info_list = client.search()
        elif args.worksheet_spec:
            client, worksheet_info = self.parse_client_worksheet_info(args.worksheet_spec)
            bundle_info_list = self.get_distinct_bundles(worksheet_info)
            source = ' from worksheet %s' % (worksheet_info['name'],)
        else:
            worksheet_info = self.get_current_worksheet_info()
            if not worksheet_info:
                client = self.manager.current_client()
                bundle_info_list = client.search()
            else:
                bundle_info_list = self.get_distinct_bundles(worksheet_info)
                source = ' from worksheet %s' % (worksheet_info['name'],)
        if bundle_info_list:
            print 'Bundles%s:\n' % (source,)
            columns = ('uuid', 'name', 'bundle_type', 'state')
            bundle_dicts = [
              {col: info.get(col, info['metadata'].get(col, '')) for col in columns}
              for info in bundle_info_list
            ]
            self.print_table(columns, bundle_dicts)
        else:
            print 'No bundles%s found.' % (source,)

    def do_info_command(self, argv, parser):
        parser.add_argument('bundle_spec', help='identifier: [<uuid>|<name>]')
        parser.add_argument(
          '-p', '--parents',
          action='store_true',
          help="print only a list of this bundle's parents",
        )
        parser.add_argument(
          '-c', '--children',
          action='store_true',
          help="print only a list of this bundle's children",
        )
        parser.add_argument(
          '-v', '--verbose',
          action='store_true',
          help="print top-level contents of bundle"
        )
        args = parser.parse_args(argv)
        if args.parents and args.children:
            raise UsageError('Only one of -p and -c should be used at a time!')

        client = self.manager.current_client()
        bundle_spec = args.bundle_spec

        info = client.info(bundle_spec, parents=True, children=args.children)

        def wrap2(string):
            return '** ' + string + ' **'
        def wrap1(string):
            return '* ' + string + ' *'

        if info['parents']:
            print wrap2('Parents')
            print '\n'.join(info['parents']) + '\n'

        if args.children:
            if info['children']:
                print '\n'.join(info['children'])
        elif not args.parents:
            print wrap2('Info')
            print self.format_basic_info(info) + '\n'
        
        # Verbose output
        if args.verbose:
            (directories, files) = client.ls(self.parse_target(bundle_spec))


            print wrap2('Bundle Contents')

            # Print contents of each top-level directory in bundle
            for dir in directories:
                new_path = os.path.join(bundle_spec, dir)
                # TODO note many server calls
                (ds, fs) = client.ls(self.parse_target(new_path))
                print wrap1(dir + '/')
                self.print_ls_output(ds, fs)

            # Print first 10 lines of each top-level file in bundle
            for file in files:
                new_path = os.path.join(bundle_spec, file)
                lines = client.head(self.parse_target(new_path))

                print wrap1(file)
                print "".join(lines)

    def format_basic_info(self, info):
        metadata = collections.defaultdict(lambda: None, info['metadata'])
        # Format some simple fields of the basic info string.
        fields = {
          'bundle_type': info['bundle_type'],
          'uuid': info['uuid'],
          'data_hash': info['data_hash'] or '<no hash>',
          'state': info['state'],
          'name': metadata['name'] or '<no name>',
          'description': metadata['description'] or '<no description>',
        }
        # Format statistics about this bundle - creation time, runtime, size, etc.
        stats = []
        if 'created' in metadata:
            stats.append('Created: %s' % (self.time_str(metadata['created']),))
        if 'data_size' in metadata:
            stats.append('Size:    %s' % (self.size_str(metadata['data_size']),))
        fields['stats'] = 'Stats:\n  %s\n' % ('\n  '.join(stats),) if stats else ''
        # Compute a nicely-formatted list of hard dependencies. Since this type of
        # dependency is realized within this bundle as a symlink to another bundle,
        # label these dependencies as 'references' in the UI.
        fields['hard_dependencies'] = ''
        if info['hard_dependencies']:
            deps = info['hard_dependencies']
            if len(deps) == 1 and not deps[0]['child_path']:
                fields['hard_dependencies'] = 'Reference:\n  %s\n' % (
                  path_util.safe_join(deps[0]['parent_uuid'], deps[0]['parent_path']),)
            else:
                fields['hard_dependencies'] = 'References:\n%s\n' % ('\n'.join(
                  '  %s:%s' % (
                    dep['child_path'],
                    path_util.safe_join(dep['parent_uuid'], dep['parent_path']),
                  ) for dep in sorted(deps, key=lambda dep: dep['child_path'])
                ))
        # Compute a nicely-formatted failure message, if this bundle failed.
        # It is possible for bundles that are not failed to have failure messages:
        # for example, if a bundle is killed in the database after running for too
        # long then succeeds afterwards, it will be in this state.
        fields['failure_message'] = ''
        if info['state'] == State.FAILED and metadata['failure_message']:
            fields['failure_message'] = 'Failure message:\n  %s\n' % ('\n  '.join(
              metadata['failure_message'].split('\n')
            ))
        # Return the formatted summary of the bundle info.
        return '''
    {bundle_type}: {name}
    {description}
      UUID:  {uuid}
      Hash:  {data_hash}
      State: {state}
    {stats}{hard_dependencies}{failure_message}
        '''.format(**fields).strip()

    def do_ls_command(self, argv, parser):
        parser.add_argument(
          'target',
          help=self.TARGET_FORMAT
        )
        args = parser.parse_args(argv)
        target = self.parse_target(args.target)
        client = self.manager.current_client()
        (directories, files) = client.ls(target)
        self.print_ls_output(directories, files)

    def print_ls_output(self, directories, files):
        if directories:
            print '\n  '.join(['Directories:'] + list(directories))
        if files:
            print '\n  '.join(['Files:'] + list(files))

    def do_cat_command(self, argv, parser):
        parser.add_argument(
          'target',
          help=self.TARGET_FORMAT
        )
        args = parser.parse_args(argv)
        target = self.parse_target(args.target)
        client = self.manager.current_client()
        client.cat(target)

    def do_tail_command(self, argv, parser):
        parser.add_argument(
          'target',
          help=self.TARGET_FORMAT
        )
        args = parser.parse_args(argv)
        target = self.parse_target(args.target)
        client = self.manager.current_client()
        (bundle_spec, path) = target

        if path == '':
          state = client.tail_bundle(bundle_spec)
        else:
          state = client.tail_file(target)
        print 'Bundle state: ', state

    def do_wait_command(self, argv, parser):
        parser.add_argument('bundle_spec', help='identifier: [<uuid>|<name>]')
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        state = client.watch(args.bundle_spec, [])
        if state == State.READY:
            print state
        else:
            self.exit(state)

    def do_kill_command(self, argv, parser):
        parser.add_argument('bundle_spec', help='identifier: [<uuid>|<name>]')
        bundle_spec = parser.parse_args(argv).bundle_spec
        client = self.manager.current_client()
        client.kill(bundle_spec)

    #############################################################################
    # CLI methods for worksheet-related commands follow!
    #############################################################################

    def get_current_worksheet_info(self):
        '''
        Return the current worksheet's info, or None, if there is none.
        '''
        client, worksheet_uuid = self.manager.get_current_worksheet_uuid()
        if not worksheet_uuid:
            return None
        try:
            return client.worksheet_info(worksheet_uuid)
        except UsageError:
            # This worksheet must have been deleted. Print an error and clear it.
            print >> sys.stderr, 'Worksheet %s no longer exists!\n' % (worksheet_uuid,)
            self.manager.set_current_worksheet_uuid(client, None)
            return None

    def do_new_command(self, argv, parser):
        parser.add_argument('name', help='name: ' + spec_util.NAME_REGEX.pattern)
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        uuid = client.new_worksheet(args.name)
        self.manager.set_current_worksheet_uuid(client, uuid)
        print 'Switched to worksheet %s.' % (args.name,)

    def do_add_command(self, argv, parser):
        parser.add_argument(
          'bundle_spec',
          help='identifier: [<uuid>|<name>]',
          nargs='?')
        parser.add_argument(
          'worksheet_spec',
          help='identifier: [<uuid>|<name>]',
          nargs='?',
        )
        parser.add_argument(
          '-m', '--message',
          help='add a text element',
          nargs='?',
        )
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        if args.worksheet_spec:
            worksheet_info = client.worksheet_info(args.worksheet_spec)
        else:
            worksheet_info = self.get_current_worksheet_info()
            if not worksheet_info:
                raise UsageError('Specify a worksheet or switch to one with `cl work`.')
        if args.bundle_spec:
            client.add_worksheet_item(worksheet_info['uuid'], args.bundle_spec)
        if args.message:
            client.update_worksheet(worksheet_info, worksheet_util.get_current_items(worksheet_info) + [(None, args.message, None)])

    def do_work_command(self, argv, parser):
        parser.add_argument(
          'worksheet_spec',
          help='identifier: %s' % self.GLOBAL_SPEC_FORMAT,
          nargs='?',
        )
        parser.add_argument(
          '-x', '--exit',
          action='store_true',
          help='Leave the current worksheet.',
        )
        args = parser.parse_args(argv)
        if args.worksheet_spec:
            print '1'
            client, worksheet_info = self.parse_client_worksheet_info(args.worksheet_spec)
            print '2'
            self.manager.set_current_worksheet_uuid(client, worksheet_info['uuid'])
            print '3'
            print 'Switched to worksheet %s.' % (args.worksheet_spec,)
        elif args.exit:
            self.manager.set_current_worksheet_uuid(self.manager.current_client(), None)
        else:
            worksheet_info = self.get_current_worksheet_info()
            if worksheet_info:
                name = worksheet_info['name']
                print 'Currently on worksheet %s. Use `cl work -x` to leave.' % (name,)
            else:
                print 'Not on any worksheet. Use `cl new` or `cl work` to join one.'

    def do_edit_worksheet_command(self, argv, parser):
        parser.add_argument(
          'worksheet_spec',
          help='identifier: [<uuid>|<name>]',
          nargs='?',
        )
        parser.add_argument(
          '--name',
          help='new name: ' + spec_util.NAME_REGEX.pattern,
          nargs='?',
        )
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        if args.worksheet_spec:
            worksheet_info = client.worksheet_info(args.worksheet_spec)
        else:
            worksheet_info = self.get_current_worksheet_info()
            if not worksheet_info:
                raise UsageError('Specify a worksheet or switch to one with `cl work`.')
        if args.name:
            client.rename_worksheet(worksheet_info['uuid'], args.name)
        else:
            new_items = worksheet_util.request_new_items(worksheet_info)
            client.update_worksheet(worksheet_info, new_items)

    def do_list_worksheet_command(self, argv, parser):
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        worksheet_dicts = client.list_worksheets()
        if worksheet_dicts:
            print 'Listing all worksheets:\n'
            self.print_table(('uuid', 'name'), worksheet_dicts)
        else:
            print 'No worksheets found.'

    def do_print_command(self, argv, parser):
        parser.add_argument(
          'worksheet_spec',
          help='identifier: [<uuid>|<name>]',
          nargs='?',
        )
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        if args.worksheet_spec:
            worksheet_info = client.worksheet_info(args.worksheet_spec)
        else:
            worksheet_info = self.get_current_worksheet_info()
            if not worksheet_info:
                raise UsageError('Specify a worksheet or switch to one with `cl work`.')
        raw_lines = worksheet_util.get_worksheet_lines(worksheet_info)
        templetized_lines = raw_lines
        for line in templetized_lines:
            print line

    def do_rm_worksheet_command(self, argv, parser):
        parser.add_argument('worksheet_spec', help='identifier: [<uuid>|<name>]')
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        client.delete_worksheet(args.worksheet_spec)

    #############################################################################
    # LocalBundleClient-only commands follow!
    #############################################################################

    def do_cleanup_command(self, argv, parser):
        # This command only works if client is a LocalBundleClient.
        parser.parse_args(argv)
        client = self.manager.current_client()
        client.bundle_store.full_cleanup(client.model)

    def do_worker_command(self, argv, parser):
        # This command only works if client is a LocalBundleClient.
        parser.add_argument('iterations', type=int, default=None, nargs='?')
        parser.add_argument('sleep', type=int, help='Number of seconds to wait between successive polls', default=1, nargs='?')
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        worker = Worker(client.bundle_store, client.model)
        worker.run_loop(args.iterations, args.sleep)

    def do_reset_command(self, argv, parser):
        # This command only works if client is a LocalBundleClient.
        parser.add_argument(
          '--commit',
          action='store_true',
          help='reset is a no-op unless committed',
        )
        args = parser.parse_args(argv)
        if not args.commit:
            raise UsageError('If you really want to delete all bundles, use --commit')
        client = self.manager.current_client()
        client.bundle_store._reset()
        client.model._reset()
