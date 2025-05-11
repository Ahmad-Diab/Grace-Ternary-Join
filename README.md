# Grace Ternary Join

**Grace-Ternary-Join** is an implementation of ternary join strategy within a push-based execution model for graph analytical
database which concentrated on triangle queries.

<p align="center">Δ₃(R,S,T) = R(a,b) ⋈ S(b,c) ⋈ T(a,c)</p>


---
## Abstract
Triangle queries in graph analytics often incur prohibitive intermediate‐result blowup under tight memory constraints. We present Grace Ternary Join, a push‐based, external‐memory extension of the Grace hash join that processes three relations simultaneously, eliminating large intermediates. Our implementation demonstrates clear performance gains over binary‐join plans and highlights the trade‐offs of recursive partitioning under varying memory constraints. This work introduces a novel three-way join operator for triangle query.

For more details about the project, you can take a look at my [thesis](/thesis.pdf) document.

---
## 🛠️ Prerequisites

* **CMake** 3.5 or newer
* **C++ Compiler** with C++20 (or newer) support (Clang)
* **GNU Make** or **Ninja** (or your preferred CMake generator)
* **Git** (to clone the repository and initialize submodules)
* **Boost** (Boost header-only libraries and compiled modules)

## 📥 Installation & Build

1. **Clone the repository**

   ```bash
   git clone https://github.com/Ahmad-Diab/Grace-Ternary-Join.git
   cd Grace-Ternary-Join
   ```

2. **Initialize submodules**

   ```bash
   git submodule update --init --recursive
   ```

3. **Set executable permissions for scripts**

   ```bash
   chmod +x scripts/*.sh
   ```

4. **Create a build directory**

   ```bash
   mkdir build
   cd build
   ```

5. **Generate build files with CMake**

   ```bash
   cmake .. -DCMAKE_BUILD_TYPE=Release
   ```
   > **Tip**: To customize the available memory for join processing, open `CMakefile.txt` and update `MAX_MEMORY_SIZE` with the desired memory size in bytes.


6. **Compile the project**

   ```bash
   cmake --build . -- -j$(nproc)
   ```

## ✅ Running Tests

After a successful build, you can run the test suite to verify correctness:

```bash
# From the build directory
./gtest_external_join_lib
```

## 🧪 Experiments

We conducted two experiments:

1. DuckDB vs Grace-Ternary-Join
2. Grace-Binary-Join vs Grace-Ternary-Join

## 📊 Running Benchmarks

We conducted our benchmarks on five real-world graphs: `facebook`, `twitter`, `twitch`, `wiki`, `livejournal`. All benchmarks use a single thread.

> **⚠️ Warning:** If the join process crashes unexpectedly (e.g., due to insufficient disk space), the temporary directories under `/tmp/external-join-dir-*` may not be removed. To clean up any leftover files, run:
>
> ```bash
> rm -r /tmp/external-join-dir-*
> ```

---

### 1. Prepare the datasets

Ensure you’ve run:

```bash
cd scripts
./pull_datasets.sh
```

You’ll then have, for each `<tablename>`:
- `benchmark/resources/<tablename>/<tablename>.csv`
- `benchmark/resources/<tablename>/<tablename>_R.bin`

---

### 2. DuckDB

1. **Start DuckDB** (using or creating `benchmark.db`), enable timing:
   ```bash
   duckdb benchmark.db
   .timer on
   ```

2. **Load each CSV**  
   In the DuckDB prompt, for each `<tablename>`:
   ```sql
   CREATE TABLE <tablename>_table(a, b)
     AS SELECT *
     FROM read_csv_auto(
       'benchmark/resources/<tablename>/<tablename>.csv',
       delim=','
     );
   ```

3. **Run the triangle query**  
   In the same prompt, enforce single-threaded execution and run:
   ```sql
   PRAGMA threads=1;

   WITH
     R(a,b) AS (SELECT a,b FROM <tablename>_table),
     S(b,c) AS (SELECT a,b FROM <tablename>_table),
     T(a,c) AS (SELECT a,b FROM <tablename>_table)
   SELECT COUNT(*)
   FROM R, S, T
   WHERE R.b = S.b
     AND S.c = T.c
     AND R.a = T.a;
   ```
   Replace `<tablename>` with `facebook`, then repeat for `twitter`, `twitch`, `wiki`, and `livejournal`.


---

### 3. Grace-Binary-Join vs Grace-Ternary-Join

From your build directory, for each `<tablename>`:

```
# Grace-Binary-Join:
./<tablename>_binary_benchmark

# Grace-Ternary-Join:
./<tablename>_ternary_benchmark
```

Example:

```bash
./facebook_binary_benchmark
./facebook_ternary_benchmark
```

---

