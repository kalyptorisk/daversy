#!/usr/bin/env python
import os, sys, re, datetime, ConfigParser, time, shutil
import subprocess, glob, optparse, cStringIO, tempfile

from daversy.utils                import get_uuid4
from daversy.db.oracle.connection import DEFAULT_NLS_LANG

CMDLINE = 100
TOOLERR = 101
CMDERR  = 102
SQLERR  = 103
MIGERR  = 104

ALL_FILES = []

class DvsOracleTool(object):
    def execute(self, cmd, env=None):
        try:
            command = subprocess.Popen(cmd, stderr=subprocess.STDOUT,
                                            stdout=subprocess.PIPE,
                                            stdin=subprocess.PIPE, env=env)
            self.output = ''
            output = []
            command.stdin.close()
            while 1:
                temp = command.stdout.readline()
                if not temp:
                    break
                output.append(temp.rstrip())
            result = command.wait()
            self.output = '\n'.join(output).strip()
            return result
        except KeyboardInterrupt, SystemExit:
            raise
        except:
            pass
        return None

    def execute_out(self, msg, cmd, env=None):
        self.message(msg)
        command = subprocess.Popen(cmd, env=env)
        self.output = ''
        result = command.wait()
        return result

    def tempfile(self, data='', ext='.sql'):
        handle, location = tempfile.mkstemp(ext)
        os.write(handle, data)
        os.close(handle)
        location = location.replace('\\','/')
        ALL_FILES.append(location)
        return location

    def sqlplus(self, code):
        result = False
        location = self.tempfile(code)
        try:
            env = os.environ.copy()
            env['NLS_LANG'] = DEFAULT_NLS_LANG
            status = self.execute(['sqlplus', '-S', '-L', self.connectString,
                                   '@' + location], env)
            result = (status == 0)
        except KeyboardInterrupt, SystemExit:
            raise
        except:
            pass
        return result

    def check_commands(self):
        if self.execute(['sqlplus', '/?']) != 0:
            self.message('Unable to execute SQL*Plus, please check that Oracle is in the PATH.')
            self.quit(TOOLERR)

        if self.execute(['dvs']) != 0:
            self.message('Unable to execute Daversy, please ensure that it is in the PATH.')
            self.quit(TOOLERR)

        if hasattr(self, 'connectString') and not self.sqlplus(SQLPLUS_EXEC % ''):
            self.message('Unable to connect to Oracle, please check the connection string.')
            self.quit(TOOLERR)

    def message(self, msg, prefix='**'):
        if prefix:
            print prefix, msg
        else:
            print msg
        sys.stdout.flush()

    def execute_cmd(self, msg, cmd, env=None):
        self.message(msg)
        status = self.execute(cmd, env)
        if not status == 0:
            self.message('FAILED: ' + str(cmd))
            print self.output
            self.quit(CMDERR)

    def execute_ddl(self, msg, *list):
        self.message(msg)
        for ddl in list:
            if not self.sqlplus(SQLPLUS_EXEC_DDL % ddl):
                self.message('FAILED')
                print self.output
                self.quit(SQLERR)

    def execute_sql(self, msg, *list):
        self.message(msg)
        for ddl in list:
            if not self.sqlplus(SQLPLUS_EXEC % ddl):
                self.message('FAILED')
                print self.output
                self.quit(SQLERR)

    def get_version_count(self, *versions):
        sql = GETVERSIONCOUNT_SQL % "', '".join(versions)
        if not self.sqlplus(SQLPLUS_EXEC % sql):
            self.message('Failed to get version count: %s' % ','.join(versions))
            print self.output
            self.quit(SQLERR)
        match = re.match(r'VERSION_COUNT\s+\-+\s+(\d+)\s*', self.output)
        if not match:
            self.message('Failed to parse version count')
            print self.output
            self.quit(SQLERR)
        return int(match.group(1))

    def read_file(self, name):
        file = open(name, 'r')
        data = file.read()
        file.close()
        return data

    def write_file(self, name, data):
        file = open(name, 'w')
        file.write(data)
        file.close()

    def cleanup(self):
        while len(ALL_FILES):
            try:
                os.remove(ALL_FILES.pop())
            except OSError:
                pass

    def quit(self, code=0):
        self.cleanup()
        sys.exit(code)

class CleanDb(DvsOracleTool):
    def __main__(self):
        parser = optparse.OptionParser(usage='%prog clean CONNECT-STRING')
        (options, args) = parser.parse_args(sys.argv[2:])

        if not len(args) == 1:
            parser.print_help()
            self.quit(CMDLINE)

        self.connectString, = args

        self.check_commands()
        self.run()

    def run(self):
        self.execute_ddl('dropping all schema objects', DROPCODE_SQL, DROPTABLE_SQL, DROPTYPES_SQL)

class ExportDb(DvsOracleTool):
    def __main__(self):
        parser = optparse.OptionParser(usage='%prog export SOURCE-DB DUMP-FILE')
        (options, args) = parser.parse_args(sys.argv[2:])

        if not len(args) == 2:
            parser.print_help()
            self.quit(CMDLINE)

        self.connectString, dump = args
        self.check_commands()
        self.run(dump)

    def run(self, dump):
        env = os.environ.copy()
        env['NLS_LANG'] = DEFAULT_NLS_LANG

        self.execute_ddl('recreating existing types', RECREATETYPES_SQL)

        self.execute_out('extracting source   schema',
                         ['exp', self.connectString, 'file=' + dump,
                          'statistics=none'], env)

        self.execute_ddl('recreating existing types', RECREATETYPES_SQL)

class ImportDb(DvsOracleTool):
    def __main__(self):
        parser = optparse.OptionParser(usage='%prog import DUMP-FILE TARGET-DB')
        (options, args) = parser.parse_args(sys.argv[2:])

        if not len(args) == 2:
            parser.print_help()
            self.quit(CMDLINE)

        dump, self.connectString = args
        self.check_commands()
        self.run(dump)

    def run(self, dump):
        env = os.environ.copy()
        env['NLS_LANG'] = DEFAULT_NLS_LANG

        self.execute_ddl('dropping   target   schema', DROPCODE_SQL, DROPTABLE_SQL, DROPTYPES_SQL)

        self.execute_out('restoring  target   schema',
                         ['imp', self.connectString, 'file=' + dump, 'full=y'], env)

        self.execute_ddl('recompiling schema', RECOMPILE_SQL)

class CopyDb(DvsOracleTool):
    def __main__(self):
        parser = optparse.OptionParser(usage='%prog copy SOURCE-DB TARGET-DB')
        (options, args) = parser.parse_args(sys.argv[2:])

        if not len(args) == 2:
            parser.print_help()
            self.quit(CMDLINE)

        source, target = args

        self.check_commands()
        self.run(source, target)

    def run(self, source, target):
        self.connectString = target
        self.execute_sql('checking for connnection to target DB', 'SELECT 1 FROM dual')

        dump = self.tempfile(ext='.dmp')

        exp  = ExportDb()
        exp.connectString = source
        exp.run(dump)

        imp = ImportDb()
        imp.connectString = target
        imp.run(dump)

class WrapDb(DvsOracleTool):
    def __main__(self):
        parser = optparse.OptionParser(usage='%prog wrap SOURCE-STATE TARGET-STATE')
        (options, args) = parser.parse_args(sys.argv[2:])

        if not len(args) == 2:
            parser.print_help()
            self.quit(CMDLINE)

        source, target = args

        self.check_commands()
        self.run(source, target)

    def run(self, source, target):
        ddl    = self.tempfile(ext='.sql')
        filter = self.tempfile(CODEFILTER_INI, ext='.ini')

        self.execute_cmd('generating schema DDL',
                         ['dvs', 'generate', '-s', 'all',
                          '-f', filter, source, ddl])

        if not self.read_file(ddl).strip():
            self.message('no objects need to be encoded, exiting.')
            shutil.copy2(source, target)
            return

        self.execute_cmd('encoding schema DDL',
                         ['wrap', 'iname='+ddl, 'oname=' + ddl+'.enc'])
        os.remove(ddl)
        os.rename(ddl+'.enc', ddl)

        data = '\n'.join(x.rstrip() for x in open(ddl).readlines()) + '\n'
        encoded_data = WRAPCODE_REGEX.findall( data )

        self.message('extracting encoded objects')
        from daversy.state import FileState
        provider = FileState()
        state = provider.load(source)

        pkg = {}
        for source, objtype, name in encoded_data:
            if objtype in ['TYPE', 'PROCEDURE', 'FUNCTION']:
                state[objtype.lower()+'s'][name].source = source.strip() + ';\n/'

            if objtype == 'PACKAGE':
                pkg[name] = source.strip() + ';\n/\n'
            elif objtype == 'PACKAGE BODY':
                state.packages[name].source = pkg[name] + source.strip() + ';\n/'
                del pkg[name]

        # packages without body
        for name in pkg:
            state.packages[name].source = pkg[name].rstrip()

        self.message('saving updated state')
        provider.save(state, target, None)

class CreateDb(DvsOracleTool):
    def __main__(self):
        parser = optparse.OptionParser(usage='%prog create [options] CONNECT-STRING STATE')
        parser.add_option('--wrap', action='store_true', default=False, dest='wrap',
                          help='run the Oracle wrap utility on generated SQL')
        parser.add_option('--sql-type', dest='sql_type', metavar='TYPE', default='all',
                          help='generated the specified SQL type (default: "all")')
        (options, args) = parser.parse_args(sys.argv[2:])

        if not len(args) == 2:
            parser.print_help()
            self.quit(CMDLINE)

        self.connectString, state = args

        self.check_commands()
        self.run(options, state)

    def run(self, options, state):
        self.execute_cmd('getting state version', ['dvs', 'name', state])
        version = self.output

        ddl = self.tempfile()

        self.execute_cmd('generating schema DDL',
                         ['dvs', 'generate', '-s', options.sql_type, state, ddl])

        if options.wrap:
            self.execute_cmd('encoding schema DDL',
                             ['wrap', 'iname='+ddl, 'oname=' + ddl+'.enc'])
            os.remove(ddl)
            os.rename(ddl+'.enc', ddl)

        self.execute_ddl('dropping existing schema', DROPCODE_SQL, DROPTABLE_SQL, DROPTYPES_SQL)

        self.execute_ddl('creating new schema', SCHEMALOG_SQL % version,
                                                EXECSCRIPT_SQL % ddl)
        self.execute_ddl('recompiling schema', RECOMPILE_SQL)

class DiffDb(DvsOracleTool):
    def __main__(self):
        parser = optparse.OptionParser(usage='%prog [options] SOURCE TARGET')
        (options, args) = parser.parse_args(sys.argv[2:])

        if not len(args) == 2:
            parser.print_help()
            self.quit(CMDLINE)

        self.check_commands()
        input, output = args
        self.quit( self.run(input, output) )

    def run(self, input, output):
        self.execute_cmd('comparing states', ['dvs', 'compare', input, output])

        if not self.output:
            return 0

        log = cStringIO.StringIO( self.output )
        for line in log:
            for elem in MIGRATION_NEEDED:
                if elem in line and not ('@comment' in line and line.startswith('M')):
                    return 2
        return 1

class MigrateDb(DvsOracleTool):
    def __main__(self):
        parser = optparse.OptionParser(usage='%prog migrate [options] CONNECT-STRING STATE FILTER MIGRATION-DIR')
        parser.add_option('--wrap', action='store_true', default=False, dest='wrap',
                          help='run the Oracle wrap utility on generated SQL')
        parser.add_option('-i', dest='include_tags', default='all', metavar='TAGS',
                          help='include objects matching specified TAGS from filter (default: "all")')
        parser.add_option('-x', dest='exclude_tags', default='ignore', metavar='TAGS',
                          help='exclude objects matching specified TAGS from filter (default: "ignore")')
        parser.add_option('--no-comment', action='store_true', default=False, dest='no_comment',
                          help='do not update the comments in the target schema')
        (options, args) = parser.parse_args(sys.argv[2:])

        if not len(args) == 4:
            parser.print_help()
            self.quit(CMDLINE)

        self.connectString, state, filter, migration_dir = args
        self.check_commands()
        if not os.path.isdir(migration_dir):
            parser.error('The migrations directory does not exist')

        self.quit( self.run(options, state, filter, migration_dir) )

    def run(self, options, state, filter, migration_dir):

        #### determine current and target versions

        self.execute_cmd('getting target state version', ['dvs', 'name', state])
        self.target_version = self.output

        self.execute_ddl('getting source state version', GETVERSION_SQL)
        match = re.match(r'SCHEMA_VERSION\s+\-+\s+([\w\-]+)\s*', self.output)
        if not match:
            self.message('CANNOT FIND SCHEMA VERSION')
            print self.output
            self.quit(SQLERR)
        self.source_version = match.group(1)
        self.start_version  = self.source_version

        if self.source_version == self.target_version:
            self.message('no migration needed')
            return 0

        self.message('%s => %s' % (self.source_version, self.target_version))

        #### determine and perform migrations

        self.do_migrations(migration_dir)

        #### replace existing schema objects which don't need migration

        code_config = self.tempfile(CODEFILTER_INI, '.ini')
        ddl = self.tempfile()

        self.execute_cmd('generating updated objects',
                         ['dvs', 'generate', '-f', code_config,
                          '-s', 'create', state, ddl])

        if options.wrap:
            self.execute_cmd('encoding schema DDL',
                             ['wrap', 'iname='+ddl, 'oname=' + ddl+'.enc'])
            os.remove(ddl)
            os.rename(ddl+'.enc', ddl)

        self.execute_ddl('dropping existing objects', DROPCODE_SQL, DROPTYPES_SQL)
        self.execute_ddl('creating updated objects', EXECSCRIPT_SQL % ddl)
        self.execute_ddl('recompiling schema', RECOMPILE_SQL)

        #### replace existing comments

        if not options.no_comment:
            self.execute_cmd('generating updated comments',
                             ['dvs', 'generate', '-s', 'comment', state, ddl])

            self.execute_ddl('applying updated comments', EXECSCRIPT_SQL % ddl)

        #### check for successful migration

        self.execute_ddl('recompiling schema', RECOMPILE_SQL)

        self.migration_check = self.tempfile(ext='.state')

        if filter:
            self.execute_cmd('extracting migrated state',
                             ['dvs', 'copy', 'oracle:' + self.connectString,
                              '-f', filter, '-i', options.include_tags,
                              '-x', options.exclude_tags, self.migration_check])
        else:
            self.execute_cmd('extracting migrated state',
                             ['dvs', 'copy', 'oracle:' + self.connectString,
                              self.migration_check])

        differ = DiffDb()
        return_val = differ.run(state, self.migration_check)

        if return_val == 2:
            self.message('migration was not successful: migrated and target schemas differ.')
            self.message(differ.output, None)
            self.execute_sql('marking unsuccessful migration',
                             REMOVESCHEMAVERSION_SQL % self.target_version)
            return return_val
        elif return_val == 1:
            self.message('warning: there were some code/comment changes!!')
            self.message(differ.output, None)

        self.execute_sql('migrated successfully to [%s]' % self.target_version,
                         UPDATESCHEMA_SQL % (self.target_version, '** migration successful **'))

        return return_val

    def do_migrations(self, dir):

        self.migrations = {}

        ### get available migrations
        file_list = glob.glob(dir + '/*.sql')

        for file in file_list:
            data = self.read_file(file)
            match = MIGRATION_REGEX.match(data)
            if not match:
                self.message('[%s] is not a valid migration.' % os.path.basename(file))
                continue
            source, target, description = match.groups()
            if target == 'next-version':
                target = self.target_version

            entry = self.migrations.setdefault(source, {})
            entry[target] = (description, [data])

        ### get the list of direct migrations
        no_migrations = ConfigParser.ConfigParser()
        no_migrations.read( os.path.join(dir, 'migration.ini') )

        for source in no_migrations.options('migrations'):
            target = no_migrations.get('migrations', source)
            entry = self.migrations.setdefault(source, {})
            entry[target] = ('** no migration needed **', [])

        ### detect bridge and find the migration path

        self.bridge         = self.get_bridge_migration(dir)
        self.migration_path = self.find_path(self.start_version)
        if not self.migration_path:
            self.message('unable to find a migration path')
            for i in range(len(self.broken_paths)):
                self.message('broken path #%d' % (i+1))
                for version in self.broken_paths[i]:
                    self.message(version, '      ')
            self.quit(MIGERR)

        ### if bridge present, modify the migrations
        if self.bridge:
            for guid, commands in self.bridge.items('actions'):
                if not guid in self.migration_path[1:]:
                    self.message('cannot find version [%s] in migration path' % guid)
                    self.quit(MIGERR)
                for command in commands.split('|'):
                    args = command.split()
                    meth = self.__class__.__dict__['bridge_'+args[0].replace('-', '_')]
                    if not meth:
                        self.message('unknown action [%s] in bridge action' % args[0])
                        self.quit(MIGERR)
                    src  = self.migration_path[self.migration_path.index(guid)-1]
                    meth(self, dir, src, guid, *args[1:])

        ### run the migrations
        for i in range(len(self.migration_path)-1):
            src, tgt  = self.migration_path[i], self.migration_path[i+1]
            desc, sql = self.migrations[src][tgt]
            if sql:
                sql.append(UPDATESCHEMA_SQL % (tgt, desc))
                self.execute_sql('running migration: [%s]' % desc, *sql)
                time.sleep(2)

        if self.bridge:
            guid, desc = self.bridge.get('bridge', 'guid'), self.bridge.get('bridge', 'desc')
            self.execute_sql('bridge successful: [%s]' % desc, UPDATESCHEMA_SQL % (guid, desc))

    def find_path(self, source):
        queue = [ (source, [source]) ]
        seen  = []
        leaf  = []

        while queue:
            version, path = queue.pop(0)
            if version == self.target_version:
                return path

            if not version in self.migrations:
                leaf.append(path)
                continue

            if version in seen:
                self.message('Circular reference for %s' % version)
                continue

            seen.append(version)
            for migration in self.migrations[version]:
                new_path = list(path)
                new_path.append(migration)
                queue.insert(0, (migration, new_path))

        self.broken_paths = leaf
        return None

    ### BRIDGE HELPERS

    def get_bridge_migration(self, dir):
        for name in glob.glob(dir + '/*.ini'):
            if name.endswith('migration.ini'):
                continue
            bridge = ConfigParser.ConfigParser()
            bridge.read(name)
            if not 'bridge' in bridge.sections():
                continue
            if not self.source_version in bridge.get('bridge', 'source').split():
                continue
            if self.get_version_count(bridge.get('bridge', 'guid')) != 0:
                continue

            self.message('detected bridge: [%s]' % bridge.get('bridge', 'desc'))
            self.start_version = bridge.get('bridge', 'rebase')
            self.message('%s => %s' % (self.start_version, self.target_version))
            return bridge

        return None

    def bridge_skip_if_present(self, dir, src, tgt, *versions):
        count = self.get_version_count(*versions)
        if count == len(versions) and count > 0:
            desc, migr = self.migrations[src][tgt]
            self.migrations[src][tgt] = (desc, [])

    def bridge_replace_script(self, dir, src, tgt, *scripts):
        desc, migr = self.migrations[src][tgt]
        self.migrations[src][tgt] = (desc, self.get_scripts(dir, scripts))

    def bridge_run_pre_script(self, dir, src, tgt, *scripts):
        desc, migr = self.migrations[src][tgt]
        sql = self.get_scripts(dir, scripts)
        sql.extend(migr)
        self.migrations[src][tgt] = (desc, sql)

    def bridge_run_post_script(self, dir, src, tgt, *scripts):
        desc, migr = self.migrations[src][tgt]
        migr.extend(self.get_scripts(dir, scripts))
        self.migrations[src][tgt] = (desc, migr)

    def bridge_describe(self, dir, src, tgt, *new_desc):
        desc, migr = self.migrations[src][tgt]
        self.migrations[src][tgt] = (' '.join(new_desc), migr)

    def get_scripts(self, dir, scripts):
        result = []
        for file in scripts:
            if os.path.isfile(os.path.join(dir, file)):
                result.append(open(os.path.join(dir, file), 'r').read())
        return result

class SyncDb(DvsOracleTool):
    def __main__(self):
        parser = optparse.OptionParser(usage='%prog sync [options] CONNECT-STRING TEMP-CONNECT-STRING BASE-DIR')
        parser.add_option('-i', dest='include_tags', default='all', metavar='TAGS',
                          help='include objects matching specified TAGS from filter (default: "all")')
        parser.add_option('-x', dest='exclude_tags', default='ignore', metavar='TAGS',
                          help='exclude objects matching specified TAGS from filter (default: "ignore")')
        (options, args) = parser.parse_args(sys.argv[2:])

        if not len(args) == 3:
            parser.print_help()
            self.quit(CMDLINE)

        self.connectString, self.tempConnectString, self.base_dir = args

        self.check_commands()
        if not os.path.isdir(self.base_dir):
            parser.error('The base directory does not exist')

        self.quit( self.run(options, self.base_dir) )

    def run(self, options, base_dir):
        # setup directory configuration

        self.config_dir     = os.path.join(base_dir, 'config')
        self.migration_dir  = os.path.join(base_dir, 'migration')
        self.filter_config  = os.path.join(self.config_dir, 'schema.ini')
        self.migration_conf = os.path.join(self.migration_dir, 'migration.ini')

        self.changelog      = os.path.join(base_dir, 'changes.txt')
        self.change_report  = os.path.join(base_dir, 'change_report.html')
        self.change_commit  = os.path.join(base_dir, 'change_report.txt')
        self.current_state  = os.path.join(base_dir, 'current.state')
        self.latest_state   = self.tempfile(ext='.state')

        self.execute_cmd('getting current version', ['dvs', 'name', self.current_state])
        self.current_version = self.output
        self.next_version    = get_uuid4()

        self.execute_cmd('extracting latest database state',
                         ['dvs', 'copy', '-f', self.filter_config,
                          '-n', self.next_version, '-i', options.include_tags,
                          '-x', options.exclude_tags, 'oracle:'+self.connectString,
                          self.latest_state])

        differ = DiffDb()
        status = differ.run(self.current_state, self.latest_state)
        if status == 0:
            self.message('no changes detected')
            return 0
        elif status == 1:
            self.message('changes detected, skipping migration check')
            self.generate_diff(self.current_state, self.latest_state)

            # mark that this requires no migration
            migration_file = open(self.migration_conf, 'a')
            migration_file.write('%s = %s\n' % (self.current_version, self.next_version))
            migration_file.close()
        elif status == 2:
            self.message('changes detected, performing migration check')
            if not self.check_migration(options):
                self.message('', None)
                self.message('migration check failed, migrations may be incomplete or invalid.')
                self.generate_diff(self.latest_state, self.migrate.migration_check)
                return 1
            self.message('', None)
            self.message('migration check succeeded')
            # update SQL files with the correct version
            for migration in glob.glob(self.migration_dir+'/*.sql'):
                data = self.read_file(migration)
                match = MIGRATION_REGEX.match(data)
                if not match:
                    continue
                source, target, description = match.groups()
                if target == 'next-version':
                    self.write_file(migration, data.replace('next-version', self.next_version))
            self.generate_diff(self.current_state, self.latest_state)
            cleaner = CleanDb()
            cleaner.connectString = self.tempConnectString
            cleaner.run()

        self.execute_sql('synchronizing version numbers',
                         UPDATESCHEMA_SQL % (self.next_version, '** migrations tested **'))

        self.message('updating the change log')
        migration_changes = []
        if hasattr(self, 'migrate') and self.migrate and self.migrate.migration_path:
            for i in range(len(self.migrate.migration_path)-1):
                s, t = self.migrate.migration_path[i], self.migrate.migration_path[i+1]
                migration_changes.append('- ' + self.migrate.migrations[s][t][0])

        current_date = datetime.datetime.now().strftime('[%Y-%m-%d %H:%M] ')

        text = current_date + self.next_version + '\n%s\n\n' % ('=' * 55)
        if migration_changes:
            text += 'Migrations:\n\n' + '\n'.join(migration_changes) + '\n\n'

        text += 'Schema Changes:\n\n' + differ.output + '\n\n\n\n'

        previous_changelog = self.read_file(self.changelog)
        self.write_file(self.changelog, text + previous_changelog)

        self.write_file(self.change_commit, "build $ver$: %s => %s\n\n%s" % \
            (self.current_version, self.next_version, differ.output))

        self.message('updating the current state with latest version')
        os.remove(self.current_state)
        os.rename(self.latest_state, self.current_state)

        return 0

    def check_migration(self, orig_options):
        options = optparse.Values()
        options.sql_type = 'all'
        options.no_comment = False
        options.wrap = False

        self.message('', None)
        self.message('creating existing state on a temporary database instance')
        self.message('', None)

        self.create = CreateDb()
        self.create.connectString = self.tempConnectString
        self.create.run(options, self.current_state)

        self.message('', None)
        self.message('running migrations on the temporary database instance')
        self.message('', None)
        self.migrate = MigrateDb()
        self.migrate.connectString = self.tempConnectString
        result = self.migrate.run(options, self.latest_state,
                                  None, self.migration_dir)

        return result == 0

    def generate_diff(self, left, right):
        self.execute_cmd('generating schema difference report',
                         ['dvs', 'compare', '--html', '--context', '10',
                          left, right])
        self.write_file(self.change_report, self.output)



MIGRATION_NEEDED = ['foreign-key', 'table', 'index', 'sequence', 'materialized-view']

SQLPLUS_EXEC = """
WHENEVER SQLERROR EXIT FAILURE ROLLBACK;
SET DEFINE off;
SET SQLBLANKLINES ON;
%s
exit
/
"""

SQLPLUS_EXEC_DDL = """
WHENEVER SQLERROR CONTINUE;
SET DEFINE off;
SET SQLBLANKLINES ON;
%s
exit
/
"""

DROPCODE_SQL = """
DECLARE CURSOR object_list IS
      SELECT   object_name, object_type
      FROM     user_objects
      WHERE    object_type IN ('PACKAGE', 'FUNCTION', 'PROCEDURE', 'TRIGGER', 'VIEW')
      AND      object_name NOT LIKE 'BIN$%';
BEGIN
   FOR rec IN object_list LOOP
      EXECUTE IMMEDIATE ' DROP ' || rec.object_type || ' "' || rec.object_name || '"';
   END LOOP;
END;
/
"""

DROPTYPES_SQL = """
DECLARE CURSOR object_list IS
      SELECT   type_name
      FROM     user_types
      ORDER BY typecode ASC;
BEGIN
   FOR rec IN object_list LOOP
      EXECUTE IMMEDIATE ' DROP TYPE "' || rec.type_name || '" FORCE ';
   END LOOP;
END;
/
"""

DROPTABLE_SQL = """
DECLARE CURSOR object_list IS
      SELECT object_name FROM user_objects WHERE object_type = 'MATERIALIZED VIEW' AND object_name NOT LIKE 'BIN$%';
BEGIN
   FOR rec IN object_list LOOP
      EXECUTE IMMEDIATE ' DROP MATERIALIZED VIEW "' || rec.object_name || '"';
   END LOOP;
END;
/
DECLARE CURSOR object_list IS
      SELECT object_name FROM user_objects WHERE object_type = 'TABLE' AND object_name NOT LIKE 'BIN$%';
BEGIN
   FOR rec IN object_list LOOP
      EXECUTE IMMEDIATE ' DROP TABLE "' || rec.object_name || '" CASCADE CONSTRAINTS PURGE';
   END LOOP;
END;
/
DECLARE CURSOR object_list IS
      SELECT object_name FROM user_objects WHERE object_type = 'SEQUENCE';
BEGIN
   FOR rec IN object_list LOOP
      EXECUTE IMMEDIATE ' DROP SEQUENCE "' || rec.object_name || '"';
   END LOOP;
END;
/
"""

RECOMPILE_SQL = """
DECLARE
  ncount INTEGER;
BEGIN
    SELECT count(*)    INTO ncount
    FROM  user_objects WHERE status <> 'VALID';
    IF ncount > 0 THEN
       dbms_utility.compile_schema(USER);
    END IF;
END;
/
"""

SCHEMALOG_SQL = """
CREATE TABLE schema_log (
    schema_version     CHAR(36),
    schema_timestamp   TIMESTAMP DEFAULT sysdate,
    schema_description VARCHAR2(255),
    CONSTRAINT schema_log_req CHECK (schema_version IS NOT NULL)
)
/
INSERT INTO schema_log(schema_version, schema_description) VALUES ('%s', '*** fresh creation ***')
/
"""

UPDATESCHEMA_SQL = """
INSERT INTO schema_log(schema_version, schema_description) VALUES ('%s', '%s')
/
"""

REMOVESCHEMAVERSION_SQL = """
DELETE FROM schema_log WHERE schema_version = '%s'
/
"""

EXECSCRIPT_SQL = """
@"%s"
/
"""

RECREATETYPES_SQL = """
DECLARE
    sql_query clob;
    sql_tbl   dbms_sql.clob_table;
    i         binary_integer;
    n         binary_integer;
BEGIN
    DECLARE CURSOR type_sql IS
        SELECT ltrim(rtrim(
                 replace(dbms_metadata.get_ddl('TYPE', type_name), '"' || user || '".'),
                 ' '||chr(10)||chr(13)), ' '||chr(10)||chr(13)) AS source
        FROM   sys.user_types WHERE  type_name NOT LIKE '%$%' ORDER BY typecode DESC, type_name;
    BEGIN
        n := 1;
        FOR rec in type_sql LOOP
            sql_tbl(n) := rec.source;
            n := n + 1;
        END LOOP;
    END;

    DECLARE CURSOR drop_types IS
        SELECT type_name FROM user_types ORDER BY typecode ASC;
    BEGIN
       FOR rec IN drop_types LOOP
          EXECUTE IMMEDIATE ' DROP TYPE "' || rec.type_name || '" FORCE ';
       END LOOP;
    END;

    i := 1;
    LOOP
       EXIT WHEN i = n;
       sql_query := sql_tbl(i);
       EXECUTE IMMEDIATE to_char(sql_query);
       i := i + 1;
    END LOOP;

    dbms_utility.compile_schema(user);
END;
/
"""

GETVERSION_SQL = """
SELECT schema_version
FROM   schema_log
WHERE  schema_timestamp = ( SELECT MAX(schema_timestamp) FROM schema_log );
"""

GETVERSIONCOUNT_SQL = """
SELECT COUNT(DISTINCT schema_version) AS version_count
FROM   schema_log
WHERE  schema_version IN ('%s');
"""

CODEFILTER_INI = """
[table]
[external-table]
[materialized-view]
[sequence]
[index]
[foreign-key]
[view]
.*=all
[stored-procedure]
.*=all
[function]
.*=all
[package]
.*=all
[trigger]
.*=all
[type]
.*=all
"""

MIGRATION_REGEX = re.compile(r'/\*\*\*\s+Source-Version:\s+([\w\-]+)\s+Target-Version:\s+([\w\-]+)\s+Description:\s+(.+?)\s+\*\*\*\/')
WRAPCODE_REGEX  = re.compile(r'(CREATE OR REPLACE (TYPE|FUNCTION|PROCEDURE|PACKAGE|PACKAGE BODY|) \"(\w+?)\" wrapped.+?)(?=\s+\/\s+)', re.S)

TOOLS = { 'sync'   : SyncDb,   'migrate' : MigrateDb, 'create' : CreateDb,
          'diff'   : DiffDb,   'clean'   : CleanDb,   'wrap'   : WrapDb,
          'import' : ImportDb, 'export'  : ExportDb,  'copy'   : CopyDb,  }

def main():
    if len(sys.argv) == 1 or sys.argv[1] not in TOOLS.keys():
        print 'Please specify a valid command (%s)' % (', '.join(TOOLS.keys()))
        sys.exit(1)

    tool = TOOLS[ sys.argv[1] ]()
    try:
        tool.__main__()
        tool.cleanup()
    except:
        tool.cleanup()
        raise

if __name__ == '__main__':
    main()
