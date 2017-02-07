import sys
import os
import os.path as osp
from time import time, localtime, asctime
import sqlite3
import fnmatch
import re
import stat
import pwd

def size_to_human(full_size):
  size = full_size
  if size >= 1024:
    unit = 'KiB'
    size /= 1024.0
    if size >= 1024:
      unit = 'MiB'
      size /= 1024.0
      if size >= 1024:
        unit = 'GiB'
        size /= 1024.0
        if size >= 1024:
          unit = 'TiB'
          size /= 1024.0
    s = '%.2f' % (size,)
    if s.endswith( '.00' ): s = s[:-3]
    elif s[-1] == '0': s = s[:-1]
    return s + ' ' + unit + ' (' + str(full_size) + ')'
  else:
    return str(size)

stat_attributes = ('st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime', 
               'st_nlink', 'st_size', 'st_uid', 'st_blksize', 'st_blocks', 
               'st_dev','st_rdev', 'st_ino')

def get_login_name(uid):
    try:
        r = pwd.getpwuid(uid)
    except KeyError:
        return (None, None)
    return (r.pw_name, unicode(r.pw_gecos))

class DirectoryContent(object):
    # For long opertaions, write a message every minute in verbose mode
    time_between_verbose_messages = 60
    
    dir_type = stat.S_IFDIR
    file_type = stat.S_IFREG
    symlink_type = stat.S_IFLNK
    
    def __init__(self, path=None, db=':memory:'):
        if path:
            self.path = unicode(osp.normpath(osp.abspath(path)))
        else:
            self.path = None
        self._db = sqlite3.connect(db, check_same_thread=False)
        #self._db.execute('PRAGMA journal_mode = MEMORY')
        self._db.execute('PRAGMA synchronous = OFF')
        #self._db.execute('PRAGMA locking_mode = EXCLUSIVE')
        self._db.execute('PRAGMA cache_size = 8192')
        self._db.execute('PRAGMA page_size = 10000')
        tables_count = self._db.execute('SELECT COUNT(*) FROM sqlite_master WHERE type="table"').fetchone()[0]
        if tables_count == 0:
            self._db.execute('CREATE TABLE paths (path TEXT, type INTEGER, parent INTEGER, %s)' % ','.join(stat_attributes))
            self._db.execute('CREATE UNIQUE INDEX path_index ON paths (path)')
            self._db.execute('CREATE INDEX st_uid_index ON paths (st_uid)')
            self._db.execute('CREATE INDEX st_mode_index ON paths (st_mode)')
            self._db.execute('CREATE INDEX st_size_index ON paths (st_size)')
            self._db.execute('CREATE INDEX st_ino_index ON paths (st_ino)')
            self._db.execute('CREATE INDEX parent_index ON paths (parent)')
            self._db.commit()
            self._db.execute('CREATE TABLE errors (path TEXT, message TEXT)')
            self._db.execute('CREATE TABLE logins (uid INTEGER PRIMARY KEY, login TEXT, name Text)')

    @staticmethod
    def raise_exception(e):
        raise e

    def read_local_path(self, exclude=None, verbose=None, files_and_root_only=False):
        try:
            if exclude:
                # join all excluded fnmatch patterns in a single regexp
                exclude = re.compile('|'.join(r'(?:%s)' % fnmatch.translate(i) for i in exclude))
            count_directories = 0
            count_files = 0
            cumulative_size = 0
            first_time = last_time = time()
            stack = [(None, '')]
            while stack:
                new_time = time()
                if new_time - last_time >= self.time_between_verbose_messages:
                    self._db.commit()
                    if verbose:
                        print >> verbose, ('%s: parsed %d files and %d '
                            'directories, cumulative size = %s') % (
                            asctime(localtime(new_time)),
                            count_files,
                            count_directories,
                            size_to_human(cumulative_size))
                        verbose.flush()
                    last_time = new_time
                parent_rowid, relative_path = stack.pop(0)
                absolute_path = osp.normpath(osp.join(self.path, relative_path))
                if exclude and exclude.match(absolute_path):
                    self._db.execute('INSERT INTO errors VALUES (?, ?)', [relative_path, 'excluded'])
                    continue
                try:
                    st = os.lstat(absolute_path)
                except OSError as e:
                    message = unicode(e)
                    self._db.execute('INSERT INTO errors VALUES (?, ?)', [relative_path, message])
                    print >> sys.stderr, 'ERROR:', message
                    continue
                values = [st.st_uid]
                values.extend(get_login_name(st.st_uid))
                self._db.execute(u'INSERT OR REPLACE INTO logins VALUES (?, ?, ?)', values)
                path_type = stat.S_IFMT(st.st_mode)
                values = [relative_path, path_type, parent_rowid] + [getattr(st,i) for i in stat_attributes]
                sql = 'INSERT OR REPLACE INTO paths VALUES (%s)' % ','.join('?' for i in xrange(len(stat_attributes) + 3))
                self._db.execute(sql, values)
                self._db.commit()
                if path_type == self.dir_type:
                    count_directories += 1
                    if files_and_root_only and relative_path:
                        continue
                    dir_rowid = self._db.execute('SELECT last_insert_rowid()').fetchone()[0]
                    try:
                        listdir = os.listdir(absolute_path)
                    except OSError as e:
                        message = unicode(e)
                        self._db.execute('INSERT INTO errors VALUES (?, ?)', [relative_path, message])
                        print >> sys.stderr, 'ERROR:', message
                    else:
                        stack[0:0] = list((dir_rowid, osp.join(relative_path,i)) for i in listdir)
                elif path_type == self.file_type:
                    count_files += 1
                    cumulative_size += st.st_size
            if verbose:
                print >> verbose, ('%s: parsed %d files and %d '
                    'directories, cumulative size = %s') % (
                    asctime(localtime(new_time)),
                    count_files,
                    count_directories,
                    size_to_human(cumulative_size))
                print >> verbose, '%s: done in %d seconds' % (asctime(localtime(new_time)), int(new_time-first_time))
                verbose.flush()
        finally:
            self._db.commit()

    def lstat(self, path):
        sql = 'SELECT %s FROM paths WHERE path = ?' % ','.join(stat_attributes)
        return self._db.execute(sql, [path]).fetchone()

    def listdir(self, path_or_rowid, is_dir=None):
        if isinstance(path_or_rowid, basestring):
            rowid = self._db.execute('SELECT rowid FROM paths WHERE path = ?', [path_or_rowid]).fetchone()
            if rowid is None:
                raise ValueError(path_or_rowid)
            rowid = rowid[0]
        else:
            rowid = path_or_rowid
        if is_dir is not None:
            select_dir = ' AND (st_mode & %d) %s 0' % (stat.S_IFDIR, ('!=' if is_dir else '='))
        else:
            select_dir = ''
        for row in self._db.execute('SELECT path FROM paths WHERE parent=?%s' % select_dir, [rowid]):
            yield osp.basename(row[0])
    
    def _import_db(self, name, sqlite, verbose=None):
        root_rowid = self._db.execute('SELECT rowid FROM paths WHERE path = ""').fetchone()[0]
        db = sqlite3.connect(sqlite)
        try:   
            # Import logins
            for row in db.execute('SELECT * FROM logins'):
                sql = 'INSERT OR REPLACE INTO logins VALUES (%s)' % ','.join('?' for i in xrange(len(row)))
                self._db.execute(sql, row)
            
            # Import errors
            for row in db.execute('SELECT * FROM errors'):
                row = list(row)
                row[0] = osp.join(name, row[0])
                sql = 'INSERT INTO errors VALUES (%s)' % ','.join('?' for i in xrange(len(row)))
                self._db.execute(sql, row)
            
            # Import paths
            type_index = 1
            parent_index = 2
            rowid_map = {}
            for row in db.execute('SELECT rowid, * FROM paths ORDER BY rowid'):
                if row[1] == '':
                    dir_rowid = self._db.execute("SELECT rowid FROM paths WHERE path='%s'" % name).fetchone()[0]
                    rowid_map[row[0]] = dir_rowid
                    continue
                other_rowid = row[0]
                row = list(row[1:])
                row[0] = osp.join(name, row[0])
                if row[parent_index] is None:
                    row[parent_index] = root_rowid
                else:
                    row[parent_index] = rowid_map[row[parent_index]]
                sql = 'INSERT INTO paths VALUES (%s)' % ','.join('?' for i in xrange(len(row)))
                self._db.execute(sql, row)
                if row[type_index] == self.dir_type:
                    my_rowid = self._db.execute('SELECT last_insert_rowid()').fetchone()[0]
                    rowid_map[other_rowid] = my_rowid
        except:
            self._db.rollback()
            raise
        else:
            self._db.commit()
        
    def import_from_parallel_parsing(self, directory, verbose=None):
        self.read_local_path(verbose=verbose, files_and_root_only=True)
        for sqlite in os.listdir(directory):
            if not sqlite.endswith('.sqlite'):
                continue
            name = sqlite[:-7]
            sqlite = osp.join(directory, sqlite)
            if verbose:
                print >> verbose, 'Import %s from %s' % (name, sqlite)
                verbose.flush()
            self._import_db(name, sqlite, verbose=verbose)
    
    @staticmethod
    def _sql_where(is_dir=None, in_dir=None):
        where = []
        if is_dir is not None:
            where.append('(st_mode & %d) %s 0' % (stat.S_IFDIR, ('!=' if is_dir else '=')))
        if in_dir is not None:
            where.append("path GLOB '%s/*'" % in_dir)
        if where:
            return ' WHERE ' + ' AND '.join(where)
        else:
            return ''
    
    def paths_count(self, in_dir=None, is_dir=None):
        sql = 'SELECT COUNT(*) FROM paths%s' % self._sql_where(is_dir=is_dir, in_dir=in_dir)
        r = self._db.execute(sql).fetchone()[0]
        if r is None:
            return 0
        return r
    
    def size_sum(self, in_dir=None, is_dir=False):
        r = self._db.execute('SELECT SUM(st_size) FROM paths%s' % self._sql_where(is_dir=is_dir, in_dir=in_dir)).fetchone()[0]
        if r is None:
            return 0
        return r
    
    def size_sum_by_month(self, in_dir=None, is_dir=None):
        sql = "SELECT strftime('%%Y-%%m', st_mtime, 'unixepoch') as month, SUM(st_size) FROM paths%s GROUP BY month ORDER BY month" % self._sql_where(is_dir=is_dir, in_dir=in_dir)
        return self._db.execute(sql).fetchall()
    
    def size_sum_by_uid(self, in_dir=None, is_dir=None):
        sql = "SELECT st_uid as month, SUM(st_size) FROM paths%s GROUP BY st_uid" % self._sql_where(is_dir=is_dir, in_dir=in_dir)
        return self._db.execute(sql).fetchall()
    

    #def differences(self, other,
                    #time_delta=0, reverse=False, verbose=None):

        #if verbose:
            #print >> verbose, 'Comparison of local and remote directories content'
            #verbose.flush()
        #if reverse:
            #source_directories = dict(other.directories)
            #source_files = dict(other.files)
            #dest_directories = dict(self.directories)
            #dest_files = dict(self.files)
        #else:
            #source_directories = dict(self.directories)
            #source_files = dict(self.files)
            #dest_directories = dict(other.directories)
            #dest_files = dict(other.files)

        #modified_files = dict()
        #deleted_files = []
        #for path, dest_status in dest_files.iteritems():
            #source_status = source_files.pop(path, None)
            #if source_status is None:
                #deleted_files.append(path)
            #elif (dest_status.st_size != source_status.st_size or
                #abs(int(dest_status.st_mtime) - int(source_status.st_mtime)) >
                    #time_delta):
                #modified_files[path] = source_status
        #new_files = source_files

        #deleted_directories = []
        #for path, dest_status in dest_directories.iteritems():
            #source_status = source_directories.pop(path, None)
            #if source_status is None:
                #deleted_directories.append(path)
        #new_directories = source_directories

        #if verbose:
            #print >> verbose, '  %d new files' % len(new_files)
            #print >> verbose, '  %d modified files' % len(modified_files)
            #print >> verbose, '  %d new directories' % len(new_directories)
            #print >> verbose, '  %d deleted directories' % len(deleted_directories)
            #verbose.flush()
        #return (new_files, modified_files, new_directories,
                #deleted_directories, deleted_files)
