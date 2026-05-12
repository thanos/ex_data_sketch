defmodule ExDataSketch.Hash.Metadata do
  @moduledoc """
  Shared hash + sketch metadata block.

  Every sketch persisted by ExDataSketch v0.8.0 and later embeds this metadata
  block. The block records exactly which hash algorithm and seed produced
  the sketch, which sketch family it belongs to, and which serialization
  versions are in play. This is the foundation of:

  - merge safety (rejecting merges between sketches built with different
    hashes or seeds);
  - corruption detection (Phase 2 wraps this block in a CRC-checked frame);
  - cross-platform reproducibility (sketches built on one host can be merged
    on another only when their metadata blocks are compatible);
  - future interoperability work (Apache DataSketches, etc.).

  This module is the **Phase 1 building block** consumed by
  `ExDataSketch.Binary.Header` in Phase 2. Phase 1 does not yet rewrite the
  EXSK codec; Phase 2 will do that and stamp this block into the header.

  ## Binary Layout (metadata-block v1)

  All multi-byte integers are little-endian.

      Offset  Size    Field
      ------  ------  -----
      0       1       block_version (u8 = 1)
      1       1       hash_algorithm (u8: 0=phash2, 1=xxhash3, 2=murmur3, 255=custom)
      2       8       hash_seed (u64)
      10      1       sketch_family (u8, matches EXSK sketch_id)
      11      1       sketch_family_version (u8)
      12      1       backend_type (u8: 0=unspecified, 1=pure, 2=rust)
      13      1       flags (u8; reserved, must be 0 in v1)
      14      2       extension_size (u16) — number of trailing extension bytes
      16      N       extension bytes (forward-compat; ignored on decode in v1)

  Total: `16 + extension_size` bytes. v1 writers MUST emit `extension_size == 0`.
  v1 readers MUST round-trip unknown extension bytes verbatim on re-encode
  (forward compatibility).

  Block version is **independent** of the EXSK frame version: bumping one
  does not require bumping the other.
  """

  alias ExDataSketch.Errors.DeserializationError

  @block_version 1

  @algo_phash2 0
  @algo_xxhash3 1
  @algo_murmur3 2
  @algo_custom 255

  @backend_unspecified 0
  @backend_pure 1
  @backend_rust 2

  @max_u64 0xFFFFFFFFFFFFFFFF
  @max_u16 0xFFFF

  @type algorithm :: :phash2 | :xxhash3 | :murmur3 | :custom
  @type backend :: :unspecified | :pure | :rust

  @type t :: %__MODULE__{
          block_version: pos_integer(),
          algorithm: algorithm(),
          seed: non_neg_integer(),
          sketch_family: non_neg_integer(),
          sketch_family_version: non_neg_integer(),
          backend: backend(),
          flags: non_neg_integer(),
          extension: binary()
        }

  defstruct block_version: @block_version,
            algorithm: :xxhash3,
            seed: 0,
            sketch_family: 0,
            sketch_family_version: 0,
            backend: :unspecified,
            flags: 0,
            extension: <<>>

  @doc """
  Returns the current metadata block version.

  ## Examples

      iex> ExDataSketch.Hash.Metadata.block_version()
      1

  """
  @spec block_version() :: pos_integer()
  def block_version, do: @block_version

  @doc """
  Builds a metadata struct from explicit fields.

  ## Examples

      iex> meta = ExDataSketch.Hash.Metadata.new(:xxhash3, 0, 1, 1, :pure)
      iex> meta.algorithm
      :xxhash3
      iex> meta.sketch_family
      1

  """
  @spec new(algorithm(), non_neg_integer(), non_neg_integer(), non_neg_integer(), backend()) ::
          t()
  def new(algorithm, seed, sketch_family, sketch_family_version, backend)
      when is_atom(algorithm) and is_integer(seed) and seed >= 0 and
             is_integer(sketch_family) and sketch_family >= 0 and
             is_integer(sketch_family_version) and sketch_family_version >= 0 and
             is_atom(backend) do
    %__MODULE__{
      block_version: @block_version,
      algorithm: validate_algorithm!(algorithm),
      seed: seed,
      sketch_family: sketch_family,
      sketch_family_version: sketch_family_version,
      backend: validate_backend!(backend),
      flags: 0,
      extension: <<>>
    }
  end

  @doc """
  Encodes a metadata struct to its versioned binary representation.

  ## Examples

      iex> meta = ExDataSketch.Hash.Metadata.new(:xxhash3, 0, 1, 1, :rust)
      iex> bin = ExDataSketch.Hash.Metadata.encode(meta)
      iex> byte_size(bin)
      16

  """
  @spec encode(t()) :: binary()
  def encode(%__MODULE__{} = meta) do
    seed = clamp_u64(meta.seed)
    sketch_family = clamp_u8!(meta.sketch_family, :sketch_family)
    family_version = clamp_u8!(meta.sketch_family_version, :sketch_family_version)
    flags = clamp_u8!(meta.flags, :flags)
    extension = meta.extension || <<>>

    if byte_size(extension) > @max_u16 do
      raise ArgumentError,
            "extension is too large: #{byte_size(extension)} bytes (max #{@max_u16})"
    end

    <<
      @block_version::unsigned-8,
      algorithm_to_byte(meta.algorithm)::unsigned-8,
      seed::unsigned-little-64,
      sketch_family::unsigned-8,
      family_version::unsigned-8,
      backend_to_byte(meta.backend)::unsigned-8,
      flags::unsigned-8,
      byte_size(extension)::unsigned-little-16,
      extension::binary
    >>
  end

  @doc """
  Decodes a metadata binary into a `{t(), rest}` pair on success.

  Returns `{:ok, metadata, rest_binary}` so the caller (e.g. the binary header
  parser in Phase 2) can continue consuming bytes after the metadata block.

  Returns `{:error, %DeserializationError{}}` if the binary is malformed or
  references an unknown algorithm/backend, or carries an unsupported block
  version.

  ## Examples

      iex> meta = ExDataSketch.Hash.Metadata.new(:murmur3, 9001, 3, 2, :pure)
      iex> bin = ExDataSketch.Hash.Metadata.encode(meta)
      iex> {:ok, decoded, <<>>} = ExDataSketch.Hash.Metadata.decode(bin)
      iex> decoded.algorithm
      :murmur3
      iex> decoded.seed
      9001
      iex> decoded.sketch_family
      3

  """
  @spec decode(binary()) :: {:ok, t(), binary()} | {:error, Exception.t()}
  def decode(<<
        block_version::unsigned-8,
        algo_byte::unsigned-8,
        seed::unsigned-little-64,
        sketch_family::unsigned-8,
        family_version::unsigned-8,
        backend_byte::unsigned-8,
        flags::unsigned-8,
        ext_size::unsigned-little-16,
        rest::binary
      >>) do
    cond do
      block_version > @block_version ->
        {:error,
         DeserializationError.exception(
           reason:
             "unsupported metadata block_version #{block_version}, max supported: #{@block_version}"
         )}

      byte_size(rest) < ext_size ->
        {:error,
         DeserializationError.exception(
           reason:
             "metadata extension truncated: declared #{ext_size} bytes, only #{byte_size(rest)} available"
         )}

      true ->
        with {:ok, algorithm} <- algorithm_from_byte(algo_byte),
             {:ok, backend} <- backend_from_byte(backend_byte) do
          <<extension::binary-size(^ext_size), tail::binary>> = rest

          meta = %__MODULE__{
            block_version: block_version,
            algorithm: algorithm,
            seed: seed,
            sketch_family: sketch_family,
            sketch_family_version: family_version,
            backend: backend,
            flags: flags,
            extension: extension
          }

          {:ok, meta, tail}
        end
    end
  end

  def decode(_bin) do
    {:error,
     DeserializationError.exception(reason: "metadata block truncated (need at least 16 bytes)")}
  end

  @doc """
  Returns the wire-byte for a hash algorithm atom.

  ## Examples

      iex> ExDataSketch.Hash.Metadata.algorithm_to_byte(:xxhash3)
      1

  """
  @spec algorithm_to_byte(algorithm()) :: 0 | 1 | 2 | 255
  def algorithm_to_byte(:phash2), do: @algo_phash2
  def algorithm_to_byte(:xxhash3), do: @algo_xxhash3
  def algorithm_to_byte(:murmur3), do: @algo_murmur3
  def algorithm_to_byte(:custom), do: @algo_custom

  def algorithm_to_byte(other) do
    raise ArgumentError, "unknown hash algorithm: #{inspect(other)}"
  end

  @doc """
  Returns the atom for a hash algorithm wire-byte.

  ## Examples

      iex> ExDataSketch.Hash.Metadata.algorithm_from_byte(1)
      {:ok, :xxhash3}

      iex> {:error, _} = ExDataSketch.Hash.Metadata.algorithm_from_byte(7)

  """
  @spec algorithm_from_byte(byte()) :: {:ok, algorithm()} | {:error, Exception.t()}
  def algorithm_from_byte(@algo_phash2), do: {:ok, :phash2}
  def algorithm_from_byte(@algo_xxhash3), do: {:ok, :xxhash3}
  def algorithm_from_byte(@algo_murmur3), do: {:ok, :murmur3}
  def algorithm_from_byte(@algo_custom), do: {:ok, :custom}

  def algorithm_from_byte(other) do
    {:error,
     DeserializationError.exception(reason: "unknown hash algorithm byte #{inspect(other)}")}
  end

  @doc """
  Returns the wire-byte for a backend atom.
  """
  @spec backend_to_byte(backend()) :: 0 | 1 | 2
  def backend_to_byte(:unspecified), do: @backend_unspecified
  def backend_to_byte(:pure), do: @backend_pure
  def backend_to_byte(:rust), do: @backend_rust

  def backend_to_byte(other) do
    raise ArgumentError, "unknown backend: #{inspect(other)}"
  end

  @doc """
  Returns the atom for a backend wire-byte.
  """
  @spec backend_from_byte(byte()) :: {:ok, backend()} | {:error, Exception.t()}
  def backend_from_byte(@backend_unspecified), do: {:ok, :unspecified}
  def backend_from_byte(@backend_pure), do: {:ok, :pure}
  def backend_from_byte(@backend_rust), do: {:ok, :rust}

  def backend_from_byte(other) do
    {:error, DeserializationError.exception(reason: "unknown backend byte #{inspect(other)}")}
  end

  # -- private helpers --

  defp validate_algorithm!(algo) when algo in [:phash2, :xxhash3, :murmur3, :custom], do: algo

  defp validate_algorithm!(other) do
    raise ArgumentError, "unknown hash algorithm: #{inspect(other)}"
  end

  defp validate_backend!(b) when b in [:unspecified, :pure, :rust], do: b

  defp validate_backend!(other) do
    raise ArgumentError, "unknown backend: #{inspect(other)}"
  end

  defp clamp_u64(n) when is_integer(n) and n >= 0, do: Bitwise.band(n, @max_u64)

  defp clamp_u8!(n, _field) when is_integer(n) and n >= 0 and n <= 255, do: n

  defp clamp_u8!(n, field) do
    raise ArgumentError, "#{field} out of u8 range: #{inspect(n)}"
  end
end
