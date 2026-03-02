# Quick Start

ExDataSketch provides streaming probabilistic data structures for Elixir.
This guide covers the basics of using HLL (cardinality estimation),
CMS (frequency estimation), and Theta (set operations on cardinalities).

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

## Theta Sketch -- Set Operations on Cardinalities

Theta answers: "approximately how many distinct items, with support for
set union and intersection?"

```elixir
# Create a new Theta sketch with default k=4096
sketch = ExDataSketch.Theta.new()

# Add items
sketch = ExDataSketch.Theta.update(sketch, "user_123")
sketch = ExDataSketch.Theta.update(sketch, "user_456")

# Estimate distinct count
ExDataSketch.Theta.estimate(sketch)

# Add many items at once
sketch = ExDataSketch.Theta.update_many(sketch, ["a", "b", "c", "d"])

# Compact for serialization or merging
sketch = ExDataSketch.Theta.compact(sketch)
```

## Merging Sketches

HLL, CMS, and Theta all support merging, which is essential for distributed systems:

```elixir
# Merge HLL sketches from different nodes
combined = ExDataSketch.HLL.merge(sketch_from_node1, sketch_from_node2)
ExDataSketch.HLL.estimate(combined)

# Merge CMS sketches
combined = ExDataSketch.CMS.merge(sketch_from_node1, sketch_from_node2)
ExDataSketch.CMS.estimate(combined, "some_item")

# Merge Theta sketches
combined = ExDataSketch.Theta.merge(sketch_from_node1, sketch_from_node2)
ExDataSketch.Theta.estimate(combined)

# Merge many sketches at once (works with all sketch types)
merged = ExDataSketch.HLL.merge_many([sketch1, sketch2, sketch3])
```

## Serialization

Sketches can be serialized to binaries for storage or transmission:

```elixir
# ExDataSketch-native format (all sketch types)
binary = ExDataSketch.HLL.serialize(sketch)
{:ok, sketch} = ExDataSketch.HLL.deserialize(binary)

# Apache DataSketches CompactSketch format (Theta only, for cross-language interop)
binary = ExDataSketch.Theta.serialize_datasketches(theta_sketch)
{:ok, sketch} = ExDataSketch.Theta.deserialize_datasketches(binary)
```

## Next Steps

See the [Usage Guide](usage_guide.md) for detailed documentation on options,
backends, serialization formats, and error handling.
