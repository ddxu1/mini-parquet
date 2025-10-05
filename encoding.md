# Columnar File Format (`.colf`)

A compact, column-oriented binary format inspired by Apache Parquet.

---

## 📦 File Layout Overview

| Section | Description | Size |
|----------|--------------|------|
| **1. Header** | Magic bytes + version + column/row counts | 13 bytes |
| **2. Column Index** | Offsets for metadata/data per column | 24 × N bytes |
| **3. Schema Metadata** | Column names, types, nullability | Variable |
| **4. Column Data** | Encoded values per column | Variable |

---

## 🧱 Section 1: Header

| Offset | Bytes | Meaning |
|---------|--------|---------|
| 0–3 | `43 4F 4C 46` | Magic "COLF" |
| 4 | Version (byte) |
| 5–8 | Column Count (Int) |
| 9–12 | Row Count (Int) |

---

## 📇 Section 2: Column Index (24 bytes per column)

| Offset | Bytes | Type | Meaning |
|---------|--------|------|---------|
| 0–7 | `long` | Metadata Offset |
| 8–15 | `long` | Data Offset |
| 16–19 | `int` | Data Size (bytes) |
| 20–23 | `int` | Reserved/Padding |

---

## 🧩 Section 3: Schema Metadata

| Offset | Bytes | Type | Meaning |
|---------|--------|------|---------|
| 0–3 | `int` | Name length |
| 4–N | UTF-8 | Column name |
| N+1 | `byte` | Type code (1 = Int, 2 = String, 3 = Boolean) |
| N+2 | `byte` | Nullable (0/1) |

---

## 🧮 Section 4: Column Data

| Component | Description |
|------------|-------------|
| 4 bytes (Int) | Data size |
| N bytes | Concatenated encoded values |

---
