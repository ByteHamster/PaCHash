# Object store examples

This folder contains examples that showcase how to use the object store.
The examples are also used in the paper to showcase real-world data sets.

| File name     | Description |
| :------------ | :---------- |
| example.cpp   | Most basic example. Constructs an object store and queries a key. |
| twitter.cpp   | Reads tweets from a file into an `std::vector` and passes it to the object store for construction. |
| query.cpp     | Queries an existing object store. Keeps multiple queries in flight at the same time to maximize throughput. |
| uniprot.cpp   | During construction, just keeps pointers to a memory mapped file. Cleans up the data that is actually stored on-the-fly to reduce RAM usage. |
| wikipedia.cpp | During construction, just keeps pointers to a memory mapped file. Compresses the data that is actually stored on-the-fly to reduce RAM usage. |
