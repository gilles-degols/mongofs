### Requirements:
Tested Operating Systems: Centos 7 

Tested MongoDB: 3.6. Should be able to run easily since 3.0. Works with sharding and replica set.

Tested Python version: 3.6.

### General information
Mount a Mongo database as a FUSE file system. Purpose of this implementation is the following:
1. Allows infinite scaling, up to Petabytes of data.
2. Avoid limitations of basic file systems by allowing the following: 
   - You can put millions of files in the same directory
   - Infinite hierarchy
   - Automatic redundancy 
   - Automatic compression
   - Easier setup than HDFS (and more appropriate for small files)
   - Faster creation/deletion of millions of files 
   - Super-fast listing of files, and computation of total directory size.
   - No "advanced" problems that you could find with inodes, ... once you start to have millions of files.

Features development:
- [x] Directory: creation, deletion, listing of files
- [x] File: creation, writing, reading, deletion
- [x] Symbolic link: creation, deletion
- [x] Manage ownership owner & group
- [x] Manage special attributes on files (for selinux)
- [x] Set access / update time of a directory / file / symbolic link
- [x] Handle rename of files / directories / links
- [x] File Lock - Experimental (based on the PID)
- [x] Unit testing
- [x] Integration test
- [x] Handling unreachable MongoDB instance
- [x] Performance improvement (caching, indexes, ...)
- [ ] Documentation for the configuration file, and the sharding
- [x] First stable release

What is not possible or recommended with MongoFS:

1. Any limitation related to the underlying FUSE file system, and the fusepy library on top of it, which includes:

  1.1. Hard links: https://github.com/libfuse/libfuse/issues/79

2. Expecting 100MB/s as writing speed. There is an overhead to decode, then store the data in MongoDB.

### Developer's guide

1. First installation

We assume that you already have a MongoDB installation, otherwise, follow the procedure described here: https://docs.mongodb.com/manual/installation/
```
git clone git@github.com:gilles-degols/mongofs.git
yum -y install https://centos7.iuscommunity.org/ius-release.rpm
yum -y install python36u fuse fuse-libs
python3.6 -m ensurepip --default-pip
python3.6 -m pip install --upgrade pip
python3.6 -m pip remove fuse # Otherwise conflicts can happen with fusepy
python3.6 -m pip install  -r requirements.txt
```

2. Run the tests

```
# Run all tests
python3.6 -m unittest discover -v

# Run all tests of a class:
python3.6 -m unittest -v test.core.test_GenericFile

# Run one specific test
python3.6 -m unittest -v test.core.test_GenericFile.TestGenericFile.test_basic_save
```

3. Mount the file system in a temporary directory

By default the configuration file is in /etc/mongofs/mongofs.json. You can give an alternative path in the command line
directly, as second argument.
```
mkdir -p /mnt/data
python3.6 -m src.main /mnt/data

# With a specific configuration filepath (absolute or relative)
python3.6 -m src.main /mnt/data conf/mongofs.json
```

4. Troubleshooting

If there was a problem during the mount, the mounting directory might have some problems (impossible to delete it, re-use it, ...):
```
rmdir /mnt/data
# rmdir: failed to remove ‘/mnt/att’: Device or resource busy
# Note: the command below might not work directly / at first attempt. Feel free to retry it.
fusermount -u /mnt/data
rmdir /mnt/data
```

5. Create a new release

Set up your environment:
```
yum install -y rpm-build
mkdir -p ~/rpmbuild/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}
```

Generate a new archive containing all the sources in Github, in the appropriate directory. Then, generate the rpm.
```
cp -r mongofs mongofs-1.0.0
tar -zcvf ~/rpmbuild/SOURCES/mongofs-1.0.0.tar.gz mongofs-1.0.0
QA_SKIP_BUILD_ROOT=1 rpmbuild -ba mongofs/spec/mongofs.spec
```
