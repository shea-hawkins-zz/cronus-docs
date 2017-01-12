import errno
import gzip
import io
import logging
import os
import stat
import struct
import sys
import tempfile

import boto3

from fuse import FuseOSError, Operations, LoggingMixIn, FUSE

import local_config as conf
import vfs


MAX_INMEM_FILESIZE = 100 * 1024 * 1024
MAX_FILE_POOL_LIMIT = 2048


class S3Client(object):
    logger = logging.getLogger('s3-client')

    def __init__(self, access_key, secret_key, bucket_name):
        self.access_key = 'AKIAINASCK2OHGK4M7RA' 
        self.secret_key = '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'
        self.bucket_name = bucket_name
        self.session = None

    def connect(self):
        self.s3 = boto3.client('s3', aws_access_key_id=self.access_key, aws_secret_access_key=self.secret_key)

    def get_file_stream(self, file_name):
        response = self.s3.get_object(Bucket=self.bucket_name, Key=file_name)
        return io.BytesIO(response['Body'].read())

    def list_objects(self, path=None):
        paginator = self.s3.get_paginator('list_objects')
        params = {'Bucket': self.bucket_name, 'Delimiter': '/'}
        if path and len(path) > 1:
            params['Prefix'] = path[1:] + '/'
        page_iterator = paginator.paginate(**params)
        result = []
        for page in page_iterator:
            result.extend([{'name': obj['Key'], 'size': obj['Size'], 'id': obj['ETag']}
                           for obj in page.get('Contents', [])])
            result.extend([{'name': obj['Prefix'], 'size': 4096, 'id': None}
                           for obj in page.get('CommonPrefixes', [])])
        return result

    def download_file(self, key, fileobj):
        self.s3.download_fileobj(Fileobj=fileobj, Bucket=self.bucket_name, Key=key)


class QGS3FS(LoggingMixIn, Operations):
    """FUSE Operations implementation class"""
    logger = logging.getLogger('qg-s3fs')

    def __init__(self, bucket, root):
        self.root = root
        self.client = S3Client(conf.AWS_ACCESS_KEY_ID, conf.AWS_SECRET_ACCESS_KEY, bucket)
        self.client.connect()
        self.vfs = None

    def _get_key(self, key):
        return '{}.gz'.format(key)

    def _full_path(self, partial):
        if partial.startswith('/'):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        return path

    def _get_gz_size(self, fileobj):
        checkpoint = fileobj.tell()
        fileobj.seek(-4, 2)
        size = struct.unpack('I', fileobj.read(4))[0]
        fileobj.seek(checkpoint)
        return size

    def _get_metadata(self, vfs_fileobj):
        self.logger.debug("_get_metadata %r", vfs_fileobj)
        metadata = vfs_fileobj.metadata
        metadata['st_size'] = vfs_fileobj.size
        return metadata

    def _download(self, path):

        self.logger.debug("_download %r", path)
        prefix = self._get_key(os.path.basename(path))
        local_file = tempfile.NamedTemporaryFile(prefix=prefix)
        self.client.download_file(path[1:], local_file)
        size = self._get_gz_size(local_file)
        local_file.seek(0)
        self.logger.debug("!!!!!!!!!!!!!!!!! Finished download %r", path)
        return gzip.GzipFile(fileobj=local_file, mode='rb'), size

    def get_metadata(self, path):
        self.logger.debug("get_metadata %r", path)
        vfs_obj = self.vfs.get(path)
        if isinstance(vfs_obj, vfs.Directory):
            return vfs_obj.metadata
        else:
            return self._get_metadata(vfs_obj)

    def getattr(self, path, fh=None):
        self.logger.debug("getattr path=%r", path)
        if path == '/' or path.endswith('/'):
            metadata = vfs.get_default_metadata()
            metadata['st_size'] = 4096
            metadata['st_mode'] = (stat.S_IFDIR | 0755)
            return metadata
        try:
            if path.endswith('csv'):
                path = path + '.gz'
                return self.get_metadata(path)
            return self.vfs.get(path).metadata if self.vfs is not None else vfs.get_default_metadata()
        except Exception:
            self.logger.exception("Error in getattr(%s):", path)
            raise FuseOSError(errno.EHOSTUNREACH)

    def readdir(self, path, fh=None):
        self.logger.debug("readdir path=%r", path)
        dirs = ['.', '..']
        strip_gz = lambda in_str: in_str[:-3] if in_str.endswith('.gz') else in_str
        if self.vfs is None:
            self.vfs = vfs.VFS(metadata=self.client.list_objects(path=path))
        elif not self.vfs.get(path).content:
            self.vfs.parse(self.client.list_objects(path=path))
        return dirs + [strip_gz(sub.name) for sub in self.vfs.get(path).content]

    def read(self, path, size, offset, fh):
        self.logger.debug("read '%s' '%i' '%i' '%s'" % (path, size, offset, fh))
        vfs_fileobj = self.vfs.get(path + ".gz")
        if vfs_fileobj.fileobj is None:
            vfs_fileobj.fileobj, size = self._download(vfs_fileobj.path)
            vfs_fileobj.size = size
            vfs_fileobj.metadata['st_size'] = size
        vfs_fileobj.fileobj.seek(offset)
        return vfs_fileobj.fileobj.read(size)

    def statfs(self, path):
        self.logger.debug("statfs %r", path)
        return {
            "f_namemax" : 512,
            "f_bsize" : 1024 * 1024,
            "f_blocks" : 1024 * 1024 * 1024,
            "f_bfree" : 1024 * 1024 * 1024,
            "f_bavail" : 1024 * 1024 * 1024,
            "f_files" : 1024 * 1024 * 1024,
            "f_favail" : 1024 * 1024 * 1024,
            "f_ffree" : 1024 * 1024 * 1024
        }

    def _get_attr_native(self, path):
        self.logger.debug("_get_attr_native %r", path)
        st = os.lstat(path)
        return {key: getattr(st, key) for key in
                ('st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid')}


def main(bucket, mountpoint, **kwargs):
    mount_options = {
        'mountpoint': os.path.abspath(mountpoint),
        'fsname':'qgs3fs',
        'allow_other': True,
        'auto_cache': True,
        'atime': False,
        'max_read': 131072,
        'max_write': 131072,
        'max_readahead': 131072,
        'direct_io': True,
        'ro': True,
        'debug': False,
        'nothreads': True,
        'foreground': True,
	'nonempty': True
    }
    if sys.platform == 'darwin':
        mount_options['volname'] = os.path.basename(mountpoint)
        mount_options['noappledouble'] = True
        mount_options['daemon_timeout'] = 3600
    else:
        mount_options['big_writes'] = True

    FUSE(QGS3FS(bucket, mountpoint), **mount_options)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser('QuantGo FUSE RO interface to S3 buckets')
    parser.add_argument('--mount-point', required=True, help='Path to mount point.')
    parser.add_argument('--bucket', required=True, help='Bucket name.')
    args = parser.parse_args()
    logging.basicConfig(level=logging.ERROR)
    sys.exit(main(args.bucket, args.mount_point))
