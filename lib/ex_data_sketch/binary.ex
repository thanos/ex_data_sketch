defmodule ExDataSketch.Binary do
  @moduledoc """
  Public facade for the EXSK binary frame.

  From `ex_data_sketch` v0.8.0 onward, every sketch's `serialize/1`
  produces an EXSK v2 frame via `encode/3` here, and every sketch's
  `deserialize/1` calls `decode/1` here. The decoder transparently
  handles both v1 (pre-0.8) and v2 frames so existing persisted sketches
  remain readable.

  The EXSK v2 frame adds, relative to v1:

  - A versioned `ExDataSketch.Hash.Metadata` block recording the exact
    hashing identity and sketch family used to produce the sketch.
  - A `family_version` byte for per-sketch state evolution.
  - A `flags` byte (reserved in v2).
  - A trailing CRC32C checksum (Castagnoli) over the entire preceding
    frame, providing detection of single-bit corruption with > 99.998%
    probability for typical sketch sizes.

  See `ExDataSketch.Binary.Header` for the exact byte layout and
  `plans/binary_contract.md` for the prose specification.

  ## Layered APIs

  - **`encode/3`** — produces an EXSK v2 frame. The default writer for
    sketches.
  - **`decode/1`** — accepts an EXSK v1 OR v2 frame and returns a
    uniform decoded representation. The default reader for sketches.
  - **`peek_version/1`** — fast version sniffer for routing without
    parsing the body.

  Each layer never crashes the BEAM on malformed input; all error
  paths return `{:error, %DeserializationError{}}`.
  """

  alias ExDataSketch.Binary.Header
  alias ExDataSketch.Codec
  alias ExDataSketch.Errors.DeserializationError
  alias ExDataSketch.Hash.Metadata

  @magic "EXSK"

  @type decoded :: %{
          version: 1 | 2,
          sketch_id: non_neg_integer(),
          family_version: non_neg_integer(),
          metadata: Metadata.t() | nil,
          params: binary(),
          state: binary()
        }

  @doc """
  Returns the magic bytes used by every EXSK frame.

  ## Examples

      iex> ExDataSketch.Binary.magic()
      "EXSK"

  """
  @spec magic() :: binary()
  def magic, do: @magic

  @doc """
  Peeks at the version byte of an EXSK frame without parsing the body.

  Returns `{:ok, version}` or `{:error, %DeserializationError{}}`.

  ## Examples

      iex> meta = ExDataSketch.Hash.Metadata.new(:xxhash3, 0, 1, 1, :rust)
      iex> bin = ExDataSketch.Binary.Header.encode(meta, <<>>)
      iex> ExDataSketch.Binary.peek_version(bin)
      {:ok, 2}

      iex> v1 = ExDataSketch.Codec.encode(1, 1, <<>>, <<>>)
      iex> ExDataSketch.Binary.peek_version(v1)
      {:ok, 1}

  """
  @spec peek_version(binary()) :: {:ok, 1 | 2} | {:error, Exception.t()}
  def peek_version(<<@magic, version::unsigned-8, _rest::binary>>) when version in [1, 2] do
    {:ok, version}
  end

  def peek_version(<<@magic, version::unsigned-8, _rest::binary>>) do
    {:error, DeserializationError.exception(reason: "unsupported EXSK frame version #{version}")}
  end

  def peek_version(<<_::binary-size(4), _::binary>>) do
    {:error, DeserializationError.exception(reason: "invalid magic bytes, expected EXSK")}
  end

  def peek_version(_) do
    {:error, DeserializationError.exception(reason: "binary too short for EXSK header")}
  end

  @doc """
  Encodes a sketch into an EXSK v2 frame.

  ## Arguments

  - `metadata` — an `ExDataSketch.Hash.Metadata` struct describing the
    hashing identity and sketch family. The struct's `sketch_family`
    and `sketch_family_version` are mirrored into the frame's
    `sketch_family` and `family_version` bytes.
  - `payload` — the sketch's serialized params + state bytes. Sketch
    modules typically concatenate their params binary and state binary
    here.
  - `opts` — optional keyword list. Recognized keys: `:flags` (must be
    `0` in v2; reserved).

  Always produces a v2 frame. The legacy `ExDataSketch.Codec.encode/4`
  remains available for callers that explicitly need a v1 frame, but
  no sketch in v0.8.0 uses it.

  ## Examples

      iex> meta = ExDataSketch.Hash.Metadata.new(:xxhash3, 0, 1, 1, :rust)
      iex> bin = ExDataSketch.Binary.encode(meta, <<10, 20, 30>>)
      iex> <<"EXSK", 2, _rest::binary>> = bin
      iex> byte_size(bin) > 0
      true

  """
  @spec encode(Metadata.t(), binary(), keyword()) :: binary()
  def encode(%Metadata{} = metadata, payload, opts \\ []) when is_binary(payload) do
    Header.encode(metadata, payload, opts)
  end

  @doc """
  Decodes an EXSK frame (v1 or v2) into a uniform map.

  The returned map always contains:

  - `:version` — `1 | 2`
  - `:sketch_id` — the sketch family byte
  - `:family_version` — for v1, defaults to `0`
  - `:metadata` — an `ExDataSketch.Hash.Metadata` struct for v2 frames,
    or `nil` for v1 frames (v1 has no metadata block)
  - `:params` — the sketch's params binary (sketch-specific layout)
  - `:state` — the sketch's state binary

  ## v1 → uniform mapping

  v1 frames carry params + state but no metadata block and no
  family_version. The decoder fills `:metadata = nil` and
  `:family_version = 0` and otherwise behaves identically to the
  pre-v0.8 `ExDataSketch.Codec.decode/1`.

  ## v2 → uniform mapping

  v2 frames carry a metadata block but **no longer carry a separate
  params binary**. The payload (between the metadata and the CRC) is a
  length-prefixed `<<params_size::u32-le, params::binary,
  state::binary>>` pair so the per-sketch parser can recover both
  segments. This convention is internal to the EXSK v2 codec and is
  documented in `plans/binary_contract.md`.

  Returns `{:ok, map}` on success or `{:error,
  %DeserializationError{}}` on failure.

  ## Examples

      iex> v1 = ExDataSketch.Codec.encode(1, 1, <<14>>, <<0, 0>>)
      iex> {:ok, decoded} = ExDataSketch.Binary.decode(v1)
      iex> decoded.version
      1
      iex> decoded.metadata
      nil
      iex> decoded.params
      <<14>>
      iex> decoded.state
      <<0, 0>>

  """
  @spec decode(binary()) :: {:ok, decoded()} | {:error, Exception.t()}
  def decode(bin) when is_binary(bin) do
    case peek_version(bin) do
      {:ok, 1} -> decode_v1(bin)
      {:ok, 2} -> decode_v2(bin)
      {:error, _} = err -> err
    end
  end

  def decode(_other) do
    {:error, DeserializationError.exception(reason: "EXSK frame must be a binary")}
  end

  # -- v1 path: delegate to legacy Codec --

  defp decode_v1(bin) do
    case Codec.decode(bin) do
      {:ok, decoded} ->
        {:ok,
         %{
           version: 1,
           sketch_id: decoded.sketch_id,
           family_version: 0,
           metadata: nil,
           params: decoded.params,
           state: decoded.state
         }}

      {:error, _} = err ->
        err
    end
  end

  # -- v2 path --

  defp decode_v2(bin) do
    with {:ok, frame} <- Header.decode(bin),
         {:ok, params, state} <- split_v2_payload(frame.payload) do
      {:ok,
       %{
         version: 2,
         sketch_id: frame.sketch_family,
         family_version: frame.family_version,
         metadata: frame.metadata,
         params: params,
         state: state
       }}
    end
  end

  defp split_v2_payload(<<params_size::unsigned-little-32, rest::binary>>)
       when byte_size(rest) >= params_size do
    <<params::binary-size(^params_size), state::binary>> = rest
    {:ok, params, state}
  end

  defp split_v2_payload(_payload) do
    {:error,
     DeserializationError.exception(
       reason: "v2 payload too short to contain params_size + params + state"
     )}
  end

  @doc """
  Encodes a sketch into an EXSK v1 (legacy) frame.

  This function produces a v1 EXSK frame suitable for v0.7.x readers
  that do not understand v2 frames. It is intended for use during
  rolling upgrades where producers write v1 while consumers migrate
  to v0.8.0+.

  The v1 format excludes the metadata block, family version, flags, and
  CRC32C trailer present in v2 frames. It is only valid for sketches
  using the `:phash2` hash strategy (the v0.7.x default).

  ## Examples

      iex> ExDataSketch.Binary.encode_v1(1, 1, <<14>>, <<0, 0>>)
      <<"EXSK", 1, 1, 1, 0, 0, 0, 14, 2, 0, 0, 0, 0, 0>>

  """
  @spec encode_v1(Codec.sketch_id(), non_neg_integer(), binary(), binary()) :: binary()
  def encode_v1(sketch_id, version, params, state) do
    Codec.encode(sketch_id, version, params, state)
  end

  @doc """
  Builds the sketch payload for an EXSK v2 frame.

  This is the inverse of the internal `split_v2_payload/1` used by
  `decode/1`. Sketch modules call it to assemble their params + state
  bytes into a single payload before handing it to `encode/3`.

  ## Examples

      iex> ExDataSketch.Binary.build_payload(<<1, 2>>, <<10, 20, 30>>)
      <<2, 0, 0, 0, 1, 2, 10, 20, 30>>

  """
  @spec build_payload(binary(), binary()) :: binary()
  def build_payload(params, state) when is_binary(params) and is_binary(state) do
    <<byte_size(params)::unsigned-little-32, params::binary, state::binary>>
  end

  @doc """
  Builds a default `ExDataSketch.Hash.Metadata` block from a sketch's
  options keyword list.

  Used by every sketch's `serialize/1` implementation in v0.8.0. The
  resulting metadata block is stamped into the v2 frame header and is
  available to callers of `decode/1` for cross-validation against the
  sketch's per-sketch params binary.

  ## Arguments

  - `sketch_family` — the EXSK sketch_id (matches `Codec.sketch_id_*/0`).
  - `family_version` — the per-sketch state-layout version (currently
    `1` for every sketch).
  - `opts` — the sketch's opts keyword list. The function reads
    `:hash_strategy` (default `:phash2`) and `:seed` (default `0`).

  ## Examples

      iex> meta = ExDataSketch.Binary.metadata_from_opts(1, 1, hash_strategy: :xxhash3, seed: 42)
      iex> meta.algorithm
      :xxhash3
      iex> meta.seed
      42
      iex> meta.sketch_family
      1

  """
  @spec metadata_from_opts(non_neg_integer(), non_neg_integer(), keyword()) :: Metadata.t()
  def metadata_from_opts(sketch_family, family_version, opts) do
    algorithm =
      case Keyword.get(opts, :hash_strategy, :phash2) do
        :custom -> :custom
        strategy when strategy in [:phash2, :xxhash3, :murmur3] -> strategy
        _ -> :phash2
      end

    seed = Keyword.get(opts, :seed, 0)
    Metadata.new(algorithm, seed, sketch_family, family_version, :unspecified)
  end
end
