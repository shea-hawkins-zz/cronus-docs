import os
import stat
import time

from fuse import fuse_get_context


def get_current_time():
    return time.mktime(time.gmtime())


def get_uid_gid():
    uid, gid, pid = fuse_get_context()
    return int(uid), int(gid)


def split_path(path):
    head, tail = os.path.split(path)
    if not head or head == '/':
        return tail,
    else:
        return split_path(head) + (tail,)


uid, gid = get_uid_gid()


def get_default_metadata():
    now = get_current_time()
    return {
            'st_uid': uid,
            'st_gid': gid,
            'st_mode': (stat.S_IFREG | 0755),
            'st_mtime': now,
            'st_atime': now,
            'st_ctime': now,
            'st_nlink': 1
        }


class Directory(object):
    size = 0

    def __init__(self, name, path):
        self.name = name
        self.path = path
        self.content = []
        now = get_current_time()
        self.metadata = {
            'st_uid': uid,
            'st_gid': gid,
            'st_size': 4096,
            'st_mode': (stat.S_IFDIR | 0755),
            'st_mtime': now,
            'st_atime': now,
            'st_ctime': now,
            'st_nlink': 1
        }

    def get(self, item_name):
        for item in self.content:
            if item.name == item_name:
                return item
        raise Exception('Not found')

    def _to_json(self):
        return {
            'name': self.name,
            'path': self.path,
            'content': [item._to_json() for item in self.content],
            'metadata': {
                'st_size': self.metadata['st_size']
            }
        }

    def __repr__(self):
        return u'{} <{}> {}'.format(self.__class__, self.path, self.name)

    def __str__(self):
        return '{} <{}> {}'.format(self.__class__, self.path, self.name)


class File(object):
    def __init__(self, name, size, path, fileobj=None):
        self.name = name
        self.path = path
        self.size = size
        self.fileobj = fileobj
        now = get_current_time()
        self.metadata = {
            'st_uid': uid,
            'st_gid': gid,
            'st_mode': (stat.S_IFREG | 0755),
            'st_mtime': now,
            'st_atime': now,
            'st_ctime': now,
            'st_nlink': 1,
            'st_size': self.size
        }

    def _to_json(self):
        return {
            'name': self.name,
            'path': self.path,
            'size': self.size,
            'fileobj': None,
            'metadata': {
                'st_size': self.metadata['st_size']
            }
        }

    def __repr__(self):
        return u'{} <{}> {}'.format(self.__class__, self.path, self.name)

    def __str__(self):
        return '{} <{}> {}'.format(self.__class__, self.path, self.name)


class VFS(object):

    def __init__(self, metadata=None):
        self.root = Directory('', '/')
        self._tree = {self.root.name: {}}
        if metadata is not None:
            self.parse(metadata)

    def add_file(self, fs_object):
        names = split_path(fs_object['name'])
        curr_dir = self.root
        curr_path = [self.root.name]
        _leaf = self._tree[self.root.name]
        for name in names[:-1]:
            if name not in _leaf:
                new_dir = Directory(name, '/'.join(curr_path + [name]))
                curr_dir.content.append(new_dir)
                curr_dir = new_dir
                _leaf[name] = {}
            else:
                curr_dir = curr_dir.get(name)
            _leaf = _leaf[name]
            curr_path.append(name)
        curr_path.append(names[-1])
        curr_dir.content.append(File(names[-1], fs_object['size'], '/'.join(curr_path)))
        _leaf[names[-1]] = names[-1]

    def add_dir(self, fs_object):
        names = split_path(fs_object['name'])
        curr_dir = self.root
        curr_path = [self.root.name]
        _leaf = self._tree[self.root.name]
        for name in names[:-1]:
            if name not in _leaf:
                new_dir = Directory(name, '/'.join(curr_path + [name]))
                curr_dir.content.append(new_dir)
                curr_dir = new_dir
                _leaf[name] = {}
            else:
                curr_dir = curr_dir.get(name)
            _leaf = _leaf[name]
            curr_path.append(name)

    def parse(self, metadata):
        for fs_object in metadata:
            if fs_object['name'].endswith('.gz'):
                self.add_file(fs_object)
            else:
                self.add_dir(fs_object)

    def get(self, path):
        if path == self.root.path:
            return self.root
        current_dir = self.root
        result = None
        for path_part in split_path(path):
            result = current_dir.get(path_part)
            current_dir = result
        return result

    def _dump(self):
        return self.root._to_json()

if __name__ == '__main__':
    import json
    d1 = [{'id': None, 'name': u'20160104/', 'size': 4096},
          {'id': None, 'name': u'20160105/', 'size': 4096},
          {'id': None, 'name': u'20160106/', 'size': 4096},
          {'id': None, 'name': u'20160107/', 'size': 4096}]
    d2 = [{'id': None, 'name': u'20160104/A/', 'size': 4096},
          {'id': None, 'name': u'20160104/B/', 'size': 4096},
          {'id': None, 'name': u'20160104/Z/', 'size': 4096}]
    d3 = [{'id': '"e50c755865bc077ac900e2f0bfea20d5"',
           'name': u'20160104/Z/Z.csv.gz',
           'size': 27840},
          {'id': '"6253a6795e418415bc339ebea5ddd023"',
           'name': u'20160104/Z/ZAGG.csv.gz',
           'size': 25578},
          {'id': '"817f791a7c28f9a10a1af6b852f638f2"',
           'name': u'20160104/Z/ZAIS.csv.gz',
           'size': 3785},
          {'id': '"77d9641c7960bc6b7ea6ca0e843b760b"',
           'name': u'20160104/Z/ZAYO.csv.gz',
           'size': 26869},
          {'id': '"2b7dd6994997cc5d421c55b9d00283e6"',
           'name': u'20160104/Z/ZBH.csv.gz',
           'size': 29554},
          {'id': '"ee4c8b22a296d093f6e109f5788319c1"',
           'name': u'20160104/Z/ZBIO.csv.gz',
           'size': 23615}]
    fs = VFS()
    fs.parse(d1)
    fs.parse(d2)
    fs.parse(d3)
    print json.dumps(fs._tree, indent=2)
