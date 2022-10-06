# PaCHash

An object store for variable-sized objects, which has small internal-memory space usage
and still guarantees a limited number of external IO operations.
For a given parameter *a*, the internal memory space usage is *2 + log(a)* bits per block.
Queries for objects of size *|x|* take constant time and fetch *1 + |x|/B + 1/a* blocks from external memory.

[<img src="https://raw.githubusercontent.com/ByteHamster/PaCHash/main/plots.png" alt="Plots preview">](https://arxiv.org/pdf/2205.04745)

### Library usage

Add the following to your `CMakeLists.txt`.

```
add_subdirectory(path/to/PaCHash)
target_link_libraries(YourTarget PRIVATE PaCHash)
```

### Building the examples

```
git clone --recursive git@github.com:ByteHamster/PaCHash.git
mkdir PaCHash/build
cd PaCHash/build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j
```

### Benchmarks

Alternative methods are implemented for benchmarking.
You can find the full benchmark scripts in an independent repository: https://github.com/ByteHamster/PaCHash-Experiments

| Method            | External memory utilization | Internal memory usage | IOs per query     |
|-------------------|-----------------------------|-----------------------|-------------------|
| PaCHash           | 100%                        | ~6 bits/page          | 1 (variable size) |
| Separator Hashing | Up to 98%¹                  | ~6 bits/page          | 1                 |
| Cuckoo Hashing    | Up to 98%¹                  | Constant              | 2 parallel        |

¹ Depending on the input distribution. The methods are originally designed for fixed size objects.
Adversarial input can therefore bring the utilization down to 51% or even make construction impossible.

### License

This code is licensed under the [GPLv3](/LICENSE).
If you use the project in an academic context or publication, please cite our paper:

```
@article{pachash2022,
  author    = {Florian Kurpicz and
               Hans{-}Peter Lehmann and
               Peter Sanders},
  title     = {PaCHash: Packed and Compressed Hash Tables},
  journal   = {CoRR},
  volume    = {abs/2205.04745},
  year      = {2022},
  doi       = {10.48550/arXiv.2205.04745}
}
```
