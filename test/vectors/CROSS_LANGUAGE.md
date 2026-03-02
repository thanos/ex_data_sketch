# Cross-Language Vector Harness Specification

This document describes how to generate and consume test vectors for verifying
ExDataSketch's DataSketches CompactSketch codec against the canonical Java
implementation.

## Java Vector Generation

A minimal Java program using `org.apache.datasketches:datasketches-java` that
produces CompactSketch binaries:

```java
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;
import java.io.*;
import java.nio.file.*;

public class ThetaVectorGenerator {
    public static void main(String[] args) throws IOException {
        int lgK = 12; // k = 4096

        // Empty sketch
        UpdateSketch empty = new UpdateSketchBuilder().setNominalEntries(1 << lgK).build();
        writeVector("theta_ds_empty_k4096.bin", empty.compact(true, null));

        // 100 items
        UpdateSketch s100 = new UpdateSketchBuilder().setNominalEntries(1 << lgK).build();
        for (int i = 0; i < 100; i++) {
            s100.update("item_" + i);
        }
        CompactSketch c100 = s100.compact(true, null);
        writeVector("theta_ds_100items_k4096.bin", c100);
        System.out.printf("100 items: estimate=%.2f, retained=%d%n",
            c100.getEstimate(), c100.getRetainedEntries(true));

        // 10000 items (triggers estimation mode)
        UpdateSketch s10k = new UpdateSketchBuilder().setNominalEntries(1 << lgK).build();
        for (int i = 0; i < 10000; i++) {
            s10k.update("item_" + i);
        }
        CompactSketch c10k = s10k.compact(true, null);
        writeVector("theta_ds_10000items_k4096.bin", c10k);
        System.out.printf("10000 items: estimate=%.2f, theta=%.6f, retained=%d%n",
            c10k.getEstimate(), c10k.getTheta(), c10k.getRetainedEntries(true));
    }

    static void writeVector(String name, CompactSketch sketch) throws IOException {
        byte[] bytes = sketch.toByteArray();
        Files.write(Paths.get("test/vectors/" + name), bytes);
        System.out.printf("Wrote %s (%d bytes)%n", name, bytes.length);
    }
}
```

**Note:** Java DataSketches uses MurmurHash3 internally, so the hash values
in the produced vectors will differ from ExDataSketch's `Hash.hash64/1`.
The vectors test deserialization and estimation — not hash value identity.

## Elixir Consumption

```elixir
# In test/ex_data_sketch_cross_language_test.exs
test "deserialize Java-produced CompactSketch" do
  binary = File.read!("test/vectors/theta_ds_100items_k4096.bin")
  # Use seed: nil to skip seed hash verification (different hash functions)
  assert {:ok, sketch} = Theta.deserialize_datasketches(binary, seed: nil)
  estimate = Theta.estimate(sketch)
  # Estimate should be close to 100 (within ~5% for k=4096)
  assert_in_delta estimate, 100.0, 100 * 0.05
end
```

## Elixir → Java Validation

```elixir
# Generate bytes for Java to consume
sketch = Theta.from_enumerable(for(i <- 0..99, do: "item_#{i}"), k: 4096)
binary = Theta.serialize_datasketches(sketch)
File.write!("test/vectors/theta_exds_100items_k4096.bin", binary)
```

```java
// Java reads and validates
byte[] bytes = Files.readAllBytes(Paths.get("test/vectors/theta_exds_100items_k4096.bin"));
CompactSketch sketch = CompactSketch.wrap(Memory.wrap(bytes));
double estimate = sketch.getEstimate();
// Should be 100.0 (exact mode, 100 < k=4096)
assert Math.abs(estimate - 100.0) < 5.0;
```

**Important:** When reading ExDataSketch-produced vectors in Java, the seed
hash will differ from Java's default (since ExDataSketch uses seed 9001 with
a different hash function). Java's `CompactSketch.wrap()` with default seed
will reject the sketch. Use `CompactSketch.wrapCompact()` or set seed checking
appropriately.

## Vector Naming

- `theta_ds_*` — Produced by Java DataSketches
- `theta_exds_*` — Produced by ExDataSketch
- `theta_v1_*` — ExDataSketch native format (EXSK)
