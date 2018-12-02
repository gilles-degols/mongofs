### Requirements:
Tested Operating Systems: Centos 7 

Tested MongoDB: 3.6. Should be able to run easily since 3.0. Works with sharding and replica set.

Tested Python version: 3.4/3.6.

### General information
Mount a Mongo database as a FUSE file system. Purpose of this implementation is the following:
1. Scaling only limited to your MongoDB installation.
2. Avoid limitations of basic file systems by allowing the following: 
   - You can put millions of files in the same directory
   - Infinite hierarchy
   - Automatic redundancy 
   - Automatic compression
   - Easier setup than HDFS (and more appropriate for small files)
   - Faster creation/deletion of millions of files 
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
- [x] Documentation for the configuration file
- [x] First stable release

What is not possible or recommended with MongoFS:

1. Any limitation related to the underlying FUSE file system, and the fusepy library on top of it, which includes:

  1.1. Hard links: https://github.com/libfuse/libfuse/issues/79

2. Expecting 100MB/s as writing speed. There is an overhead to decode, then store the data in MongoDB. But the slowest part is fusepy so we cannot improve much more the current code. Check benchmarks below if you want some numbers.

### Installation guide

1. Install the different packages
```
yum -y install https://github.com/gilles-degols/mongofs/releases/download/v1.0.0/mongofs-1.0.0-0.noarch.rpm
```

2. Mount the file system with the default parameters in /etc/mongofs/mongofs.json

For more information about the configuration parameters, check the appropriate section below.
```
sudo mongofs-mount /mnt/data
```


### Developer's guide

1. First installation

We assume that you already have a MongoDB installation, otherwise, follow the procedure described here: https://docs.mongodb.com/manual/installation/
```
git clone git@github.com:gilles-degols/mongofs.git
yum -y install python36u fuse fuse-libs
python3 -m ensurepip --default-pip
python3 -m pip install --upgrade pip
python3 -m pip remove fuse # Otherwise conflicts can happen with fusepy
python3 -m pip install  -r requirements.txt
```

2. Run the tests

```
# Run all tests
python3 -m unittest discover -v

# Run all tests of a class:
python3 -m unittest -v test.core.test_GenericFile

# Run one specific test
python3 -m unittest -v test.core.test_GenericFile.TestGenericFile.test_basic_save
```

3. Mount the file system in a temporary directory

By default the configuration file is in /etc/mongofs/mongofs.json. You can give an alternative path in the command line
directly, as second argument.
```
mkdir -p /mnt/data
python3 -m src.main /mnt/data

# With a specific configuration filepath (absolute or relative)
python3 -m src.main /mnt/data conf/mongofs.json
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

Add a tag for your version, and push it to the remote Git repository.
```
git tag -a v1.1.0 -m "Version 1.1.0"
git push origin v1.1.0
```

Set up your environment:
```
yum install -y rpm-build
mkdir -p ~/rpmbuild/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}
```

Generate a new archive containing all the sources in Github, in the appropriate directory. Then, generate the rpm.
```
cp -r mongofs mongofs-1.1.0
tar -zcvf ~/rpmbuild/SOURCES/mongofs-1.1.0.tar.gz mongofs-1.1.0
QA_SKIP_BUILD_ROOT=1 rpmbuild -ba mongofs-1.1.0/spec/mongofs.spec
```

6. Benchmarks

If you want to test the performance of MongoFS versus your file system, you can easily test the handling of big files. The numbers given below were generated on a VM with 3GB of RAM & 4vCPU hosted on a Desktop computer with 16GB of RAM & SSD. So the reading speed is not strictly related to the disk as everything might be in cache on the OS level.
Benchmarking is a difficult subject, so be careful when you compare numbers.
Be aware that FUSE + fusepy (library used in mongofs) is in fact the slowest part of writing, and unfortunately we cannot improve the performance a lot more until it is improved on their side.
```
python3 -m src.main /mnt/data conf/mongofs.json

# Test local file system
yes "a" | dd of=output.dat bs=4k count=2500000 iflag=fullblock && time cat output.dat > /dev/null
# ~80MB/s (writing), ~273MB/s (reading)

# Test MongoFS with a local MongoDB instance (small block size)
yes "a" | dd of=/mnt/data/output.dat bs=4k count=2500000 iflag=fullblock && time cat /mnt/data/output.dat > /dev/null
# ~8MB/s (writing), ~56MB/s (reading)

# Test MongoFS with a local MongoDB instance (bigger block size)
yes "a" | dd of=/mnt/data/output.dat bs=10M count=1000 iflag=fullblock && time cat /mnt/data/output.dat > /dev/null
# ~13MB/s (writing), ~56MB/s (reading)

```

### Configuration parameters

Default configuration parameters can be seen in conf/mongofs.json, every one of them must be set otherwise MongoFS will not work.

1. mongo.hosts: List of hosts of your MongoDB cluster. You can also put a list of Mongos instances if you have a sharded cluster.
2. mongo.database: Database to store the MongoFS data.
3. mongo.prefix: A prefix for the collections created by MongoFS inside the database given by "mongo.database".
4. mongo.access_attempt_s: Minimum number of seconds we will try to reconnect to MongoDB if there is a connection issue. Put 0 for infinity.
5. mongo.chunk_size: Each file is split in several chunks of the given size. Value must be between 1 bytes and 15MB.
6. cache.timeout_s: Maximum number of seconds we can keep a cache of file (so, without contacting the database). Highly recommended to have at least "1" as value. Put 0 to deactivate that functionality.
7. cache.max_elements: Maximum number of files (metadata only) we can keep in the cache.
8. data_cache.timeout_s: Maximum number of seconds we can keep a cache of file data (so, without contacting the database). Highly recommended to have at least "1" as value. Put 0 to deactivate that functionality.
9. data_cache.max_elements: Maximum number of chunks of data we can keep in the cache.
10. development: Activate the development mode if set to true, in that case the mount is in foreground, the logs are activated, and the data are wipped at every mount.
11. host: Current hostname of the machine itself (so, it should be unique), to manage file locks.
12. lock.access_attempt_s: Number of seconds we try to access to a locked file before giving up and returning an error to the client. Put 0 for infinity.
13. lock.timeout_s: Maximum number of seconds we consider a lock valid. To avoid a deadlock if a server is down, we delete the lock after that amount of time. Put 0 for infinity.


