# Quick Start

ExDataSketch provides streaming probabilistic data structures for Elixir.
This guide covers the basics of using HLL (cardinality estimation) and
CMS (frequency estimation).

> Note: Phase 0 contains stubs only. The examples below show the intended API.
> Full implementations will be available in Phase 1.

## HyperLogLog (HLL) -- Cardinality Estimation

HLL answers: "approximately how many distinct items have I seen?"

```elixir
# Create a new HLL sketch with default precision (p=14)
sketch = ExDataSketch.HLL.new()

# Add items
sketch = ExDataSketch.HLL.update(sketch, "user_123")
sketch = ExDataSketch.HLL.update(sketch, "user_456")
sketch = ExDataSketch.HLL.update(sketch, "user_123")  # duplicate

# Estimate distinct count (will be approximately 2.0)
ExDataSketch.HLL.estimate(sketch)

# Add many items at once
sketch = ExDataSketch.HLL.update_many(sketch, ["a", "b", "c", "d"])
```

## Count-Min Sketch (CMS) -- Frequency Estimation

CMS answers: "approximately how many times have I seen this item?"

```elixir
# Create a new CMS with default width and depth
sketch = ExDataSketch.CMS.new()

# Add items (default increment of 1)
sketch = ExDataSketch.CMS.update(sketch, "page_home")
sketch = ExDataSketch.CMS.update(sketch, "page_home")
sketch = ExDataSketch.CMS.update(sketch, "page_about")

# Estimate frequency
ExDataSketch.CMS.estimate(sketch, "page_home")   # approximately 2
ExDataSketch.CMS.estimate(sketch, "page_about")  # approximately 1
ExDataSketch.CMS.estimate(sketch, "page_other")  # approximately 0
```

## Merging Sketches

Both HLL and CMS support merging, which is essential for distributed systems:

```elixir
# Merge HLL sketches from different nodes
combined = ExDataSketch.HLL.merge(sketch_from_node1, sketch_from_node2)
ExDataSketch.HLL.estimate(combined)

# Merge CMS sketches
combined = ExDataSketch.CMS.merge(sketch_from_node1, sketch_from_node2)
ExDataSketch.CMS.estimate(combined, "some_item")
```

## Serialization

Sketches can be serialized to binaries for storage or transmission:

```elixir
# ExDataSketch-native format
binary = ExDataSketch.HLL.serialize(sketch)
sketch = ExDataSketch.HLL.deserialize(binary)

# DataSketches-compatible format (Theta only, for cross-language interop)
binary = ExDataSketch.Theta.serialize_datasketches(theta_sketch)
```

## Next Steps

See the [Usage Guide](usage_guide.md) for detailed documentation on options,
backends, serialization formats, and error handling.
