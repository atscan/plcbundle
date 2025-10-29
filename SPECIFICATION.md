# plcbundle V1 (draft) Specification

> ⚠️ **Preview Version - Request for Comments!**

## 1. Abstract

`plcbundle` is a system for archiving and distributing [DID PLC (Placeholder) directory](https://plc.directory) operations in a secure, verifiable, and efficient manner. It groups chronological operations from the PLC directory into immutable, compressed bundles. These bundles are cryptographically linked, forming a verifiable chain of history. This specification details the V1 format for the bundles, the index file that describes them, and the processes for creating and verifying them to ensure interoperability between implementations.

---

## 2. Key Terminology

*   **Operation:** A single DID PLC operation, as exported from a PLC directory's [`/export` endpoint](https://web.plc.directory/api/redoc#operation/Export). It is represented as a single JSON object.
*   **Bundle:** A single compressed file containing a fixed number of operations.
*   **Index:** A JSON file named `plc_bundles.json` that contains metadata for all available bundles in the repository. It is the entry point for discovering and verifying bundles.
*   **Content Hash:** The [SHA-256](https://en.wikipedia.org/wiki/SHA-2) hash of the *uncompressed* [JSONL](https://jsonlines.org/) content of a single bundle. This hash uniquely identifies the bundle's data.
*   **Chain Hash:** A cumulative SHA-256 hash that links a bundle to its predecessor, ensuring the integrity and order of the entire chain.
*   **Compressed Hash:** The SHA-256 hash of the *compressed* `.jsonl.zst` bundle file. This is used to verify file integrity during downloads.

---

## 3. Operation Order and Reproducibility

To guarantee that bundles are byte-for-byte reproducible, the order of operations must be deterministic. The PLC directory's `⁠/export` endpoint streams operations in a pre-sorted, chronological order. A compliant implementation must preserve this exact order.

---

## 4. Bundle Format

### 4.1. File Naming and Structure

*   **Naming Convention:** Bundles are named sequentially with six-digit zero-padding, following the format `%06d.jsonl.zst`.
    *   *Examples:* `000001.jsonl.zst`, `000123.jsonl.zst`.
*   **Content:** Each bundle contains exactly **10,000** PLC operations.
*   **Compression:** The JSONL content is compressed using [Zstandard](https://facebook.github.io/zstd/) (zstd).

### 4.2. Serialization and Data Integrity

To ensure that the `content_hash` is reproducible across all compliant implementations, the serialization process must be deterministic. Because the JSON specification does not guarantee the order of keys in an object, re-serializing can lead to different byte representations and hash mismatches.

Therefore, the following rules are mandatory for constructing the JSONL content:

1.  **Preserve Raw Data:** A compliant implementation **must** capture and store the exact, unmodified raw JSON byte string for each operation as it is originally received from the PLC directory's `/export` stream.

2.  **Construct JSONL:** The bundle's uncompressed content **must** be constructed by concatenating these stored raw byte strings.

3.  **Newline Termination:** Each operation's JSON byte string **must be terminated by a single newline character (`\n`, ASCII `0x0A`)**. This includes the final operation, resulting in a trailing newline at the end of the JSONL content.

Implementations **must not** unmarshal and then re-marshal the operation JSON, as this would break the deterministic nature of the content hash.

---

## 5. Index Format (`plc_bundles.json`)

The index is a single JSON file that acts as the manifest for the entire bundle repository.

### 5.1. Top-Level Index Object

| Field              | Type                      | Description                                                                                             |
| ------------------ | ------------------------- | ------------------------------------------------------------------------------------------------------- |
| `version`          | string                    | The version of the index format. For V1, this is always `"1.0"`.                                        |
| `last_bundle`      | integer                   | The bundle number of the most recent bundle in the index.                                               |
| `updated_at`       | string (RFC3339Nano)      | The timestamp in UTC when the index was last updated.                                                   |
| `total_size_bytes` | integer                   | The total size of all compressed bundle files in the repository, in bytes.                              |
| `bundles`          | array of `BundleMetadata` | An array containing metadata for each bundle, sorted by `bundle_number`.                                |

### 5.2. BundleMetadata Object

Each object in the `bundles` array describes a single bundle and contains the following fields:

| Field                | Type                   | Description                                                                                                                                                             |
| -------------------- | ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bundle_number`      | integer                | The sequential number of the bundle (e.g., `1`).                                                                                                                         |
| `start_time`         | string (RFC3339Nano)   | The `createdAt` timestamp of the first operation in the bundle.                                                                                                         |
| `end_time`           | string (RFC3339Nano)   | The `createdAt` timestamp of the last operation in the bundle.                                                                                                          |
| `operation_count`    | integer                | The number of operations in the bundle. For a complete V1 bundle, this is always `10000`.                                                                                |
| `did_count`          | integer                | The number of unique DIDs referenced in the bundle's operations.                                                                                                        |
| `hash`               | string (SHA-256)       | **(Primary Chain Hash)** The cumulative hash linking this bundle to the previous one. See Hash Calculation.                                                               |
| `content_hash`       | string (SHA-256)       | The hash of the uncompressed JSONL content of this bundle.                                                                                                              |
| `parent`             | string (SHA-256)       | The `hash` (Chain Hash) of the preceding bundle. For the first bundle, this field is an empty string.                                                                   |
| `compressed_hash`    | string (SHA-256)       | The hash of the compressed `.jsonl.zst` file.                                                                                                                           |
| `compressed_size`    | integer                | The size of the compressed bundle file in bytes.                                                                                                                        |
| `uncompressed_size`  | integer                | The size of the uncompressed JSONL content in bytes.                                                                                                                    |
| `cursor`             | string (RFC3339Nano)   | The `end_time` of the previous bundle. Used as a hint for fetching subsequent operations. For the first bundle, this is an empty string.                                  |
| `created_at`         | string (RFC3339Nano)   | The timestamp in UTC when the bundle and its metadata were created.                                                                                                     |

---

## 6. Bundle Creation and Hashing Process

The creation of a new bundle is a sequential process that ensures the integrity of the chain.

### 6.1. Collecting Operations

1.  **Mempool:** Operations are fetched from the PLC directory's [`/export` endpoint](https://web.plc.directory/api/redoc#operation/Export) and collected into a temporary staging area, or "mempool".
2.  **Chronological Validation:** The mempool must enforce that operations are added in chronological order, as described in [Section 3](#3-operation-order-and-reproducibility).
3.  **Boundary Deduplication:** To prevent including the same operation in two adjacent bundles, the system must use a "boundary CID" mechanism. When creating bundle `N+1`, it must ignore any fetched operations whose `createdAt` timestamp and `CID` match those from the very end of bundle `N`.
4.  **Filling the Mempool:** The process continues fetching and deduplicating operations until at least 10,000 are collected in the mempool.

### 6.2. Creating a Bundle File

1.  **Take Operations:** Exactly 10,000 operations are taken from the front of the mempool.
2.  **Serialize:** These operations are serialized into a single block of newline-delimited JSON ([JSONL](https://jsonlines.org/)), adhering to the integrity rules in [Section 4.2](#42-serialization-and-data-integrity).
3.  **Compress and Save:** The JSONL data is compressed using [Zstandard](https://facebook.github.io/zstd/) and saved to a file with the appropriate sequential name (e.g., `000001.jsonl.zst`).

### 6.3. Hash Calculation

Three distinct hashes are calculated for each bundle. All use the **SHA-256** algorithm.

1.  **Content Hash (`content_hash`):**
    *   This hash is calculated on the **uncompressed** JSONL data.
    *   `content_hash = SHA256(uncompressed_jsonl_data)`

2.  **Compressed Hash (`compressed_hash`):**
    *   This hash is calculated on the **compressed** `.jsonl.zst` file data.
    *   `compressed_hash = SHA256(zstd_compressed_data)`

3.  **Chain Hash (`hash`):**
    *   This is the cumulative hash that ensures the chain's integrity. It is calculated by hashing the concatenation of the parent's chain hash and the current bundle's content hash, separated by a colon.
    *   **For the Genesis Bundle (Bundle #1):**
        `hash = SHA256("plcbundle:genesis:" + content_hash)`
    *   **For Subsequent Bundles (Bundle #N > 1):**
        `hash = SHA256(parent_chain_hash + ":" + current_content_hash)`
        *(Where `parent_chain_hash` is the `hash` field from the metadata of bundle `N-1`)*.

### 6.4. Updating the Index

1.  A new `BundleMetadata` object is created for the new bundle, populated with all the information described in [Section 5.2](#52-bundlemetadata-object).
2.  This metadata object is appended to the `bundles` array in the main `Index` object.
3.  The `Index` object's top-level fields (`last_bundle`, `updated_at`, `total_size_bytes`) are updated to reflect the new state.
4.  The entire `Index` object is serialized to JSON and saved, atomically overwriting the existing `plc_bundles.json` file.

