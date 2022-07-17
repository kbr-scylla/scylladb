:term:`Sorted Strings Table (SSTable)<SSTable>` is the persistent file format used by Scylla and Apache Cassandra. SSTable is saved as a persistent, ordered, immutable set of files on disk.
Immutable means SSTables are never modified; they are created by a MemTable flush and are deleted by a compaction.
The location of Scylla SSTables is specified in scylla.yaml ``data_file_directories`` parameter (default location: ``/var/lib/scylla/data``).

SSTable 3.0 (mc format) is more efficient and requires less disk space than the SSTable 2.x. SSTable version support is as follows: 


.. list-table::
   :widths: 33 33 33
   :header-rows: 1

   * - SSTable Version
     - Scylla Enterprise Version
     - Scylla Open Source Version
   * - 3.x ('md')
     - 2021.1
     - 4.3 and above
   * - 3.0 ('mc')
     - 2019.1, 2020.1
     - 3.x, 4.1, 4.2
   * - 2.2 ('la')
     - N/A
     - 2.3
   * - 2.1.8 ('ka')
     - 2018.1
     - 2.2
     
