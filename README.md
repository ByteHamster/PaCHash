# PactHash

An object store for variable sized objects, which has small internal-memory space usage
and still guarantees a limited number of external IO operations.
Alternative methods are implemented for benchmarking.

| Method            | External memory utilization | Internal memory usage | IOs per query     |
|-------------------|-----------------------------|-----------------------|-------------------|
| PactHash          | 100%                        | ~6 bits/page          | 1 (variable size) |
| Separator Hashing | Up to 98%¹                  | ~6 bits/page          | 1                 |
| Cuckoo Hashing    | Up to 98%¹                  | Constant              | 2 parallel        |

¹ Depending on the input distribution. Adversarial input can bring the utilization down to 50%.

### Building the examples

```
git clone --recursive git@hub.com:ByteHamster/PactHash.git
mkdir PactHash/build
cd PactHash/build
cmake .. # First attempt fails because of a library dependency
cmake ..
make -j8
```

### Library usage

Add the following to your `CMakeLists.txt`.
Note that you need to run `cmake` twice because an error in a library dependency makes it fail the first time.

```
add_subdirectory(path/to/PactHash)
target_link_libraries(YourTarget PRIVATE PactHash)
```

### License

This code is licensed under the [GPLv3](/LICENSE).
If you use the project in an academic context or publication, please cite our paper.
