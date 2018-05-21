### Requirements:
Supported Operating Systems: Centos 7 

Supported MongoDB: 3.6. Should be able to run easily since 3.0. Works with sharding and replica set.

### General information
Mount a Mongo database as a FUSE file system. Purpose of this implementation is the following:
1. Allows infinite scaling, up to Petabytes of data.
2. Avoid limitations of basic file systems by allowing the following: 
   - You can put millions of files in the same directory
   - You can create files of hundreds of TBs (being stuck à 4TB with some NFS storage is not nice)
   - Infinite hierarchy
   - Automatic redundancy 
   - Easier setup than HDFS (and more appropriate for small files)
   - Faster creation/deletion of millions of files 
   - Super-fast listing of files, and computation of total directory size.
   - No "advanced" problems that you could find with inodes, ... once you start to have millions of files.

