defmodule ExDataSketch.Binary.Header do
  @moduledoc """
  EXSK v2 binary frame header.

  This module owns the layout of the v2 frame used to wrap every persisted
  sketch from `ex_data_sketch` v0.8.0 onward. It is decoupled from the
  `ExDataSketch.Codec` historical entry point so that the Phase 2 frame
  format can evolve without disturbing the v1 reader and the existing
  golden vectors.

  ## v2 Layout

  All multi-byte integers are little-endian. CRC is computed over every
  byte preceding it (offsets 0 .. crc_off - 1).

      off       size  field                          notes
        0         4   magic "EXSK"                   identical to v1
        4         1   serialization_version (u8)     = 2
        5         1   sketch_family (u8)             matches the EXSK sketch_id
        6         1   family_version (u8)            sketch-specific layout version
        7         1   flags (u8)                     reserved; must be 0 in v2
        8         2   header_size (u16 LE)           total bytes from offset 0 up to (not including) the payload
       10         M   hash_metadata block (variable) `ExDataSketch.Hash.Metadata`
     10+M         4   payload_size (u32 LE)
     14+M         N   payload                        sketch-specific (params + state encoding)
   14+M+N         4   crc32c (over bytes [0 .. 14+M+N - 1])

  Total frame size: `18 + M + N` bytes.

  The `header_size` field equals `10 + M + 4` — i.e., the offset of the
  payload. A reader can use it as a fast-skip when only the hash metadata
  is needed. Mismatch between the declared and actual header_size is a
  corruption indicator.

  ## Forward compatibility

  - Wire bytes for `serialization_version`, `magic`, and the metadata-block
    algorithm/backend bytes are stable across all v0.x releases.
  - Bumping `family_version` is the per-sketch evolution lever and does
    not affect the frame parser.
  - Future `flags` bits will be defined here, with the rule "if a reader
    encounters an unrecognized flag bit it MUST reject the frame as
    incompatible". This is intentionally strict — silent feature
    fallthrough is the cause of most binary-format bugs.

  ## See also

  - `ExDataSketch.Binary` — public facade.
  - `ExDataSketch.Binary.Validator` — defensive checks and error taxonomy.
  - `ExDataSketch.Binary.CRC` — checksum algorithm.
  - `ExDataSketch.Hash.Metadata` — embedded hashing identity block.
  - `plans/binary_contract.md` — full prose specification.
  """

  alias ExDataSketch.Binary.CRC
  alias ExDataSketch.Errors.DeserializationError
  alias ExDataSketch.Hash.Metadata

  @magic "EXSK"
  @version 2
  @max_u32 0xFFFFFFFF
  @max_u16 0xFFFF

  @type frame :: %{
          magic: binary(),
          serialization_version: 2,
          sketch_family: non_neg_integer(),
          family_version: non_neg_integer(),
          flags: non_neg_integer(),
          metadata: Metadata.t(),
          payload: binary()
        }

  @doc """
  Returns the EXSK magic bytes.

  ## Examples

      iex> ExDataSketch.Binary.Header.magic()
      "EXSK"

  """
  @spec magic() :: binary()
  def magic, do: @magic

  @doc """
  Returns the current frame version (`2`).

  ## Examples

      iex> ExDataSketch.Binary.Header.version()
      2

  """
  @spec version() :: 2
  def version, do: @version

  @doc """
  Encodes an EXSK v2 frame around the given metadata and payload.

  The metadata's `sketch_family` and `sketch_family_version` are mirrored
  into the frame's `sketch_family` / `family_version` bytes so that a
  reader can validate sketch identity without first parsing the metadata.

  Raises `ArgumentError` for malformed inputs (`payload` over 4 GiB, bad
  `flags`, etc).

  ## Examples

      iex> meta = ExDataSketch.Hash.Metadata.new(:xxhash3, 0, 1, 1, :rust)
      iex> bin = ExDataSketch.Binary.Header.encode(meta, <<1, 2, 3>>)
      iex> <<"EXSK", 2, _rest::binary>> = bin
      iex> byte_size(bin) > 0
      true

  """
  @spec encode(Metadata.t(), binary(), keyword()) :: binary()
  def encode(%Metadata{} = metadata, payload, opts \\ []) when is_binary(payload) do
    flags = Keyword.get(opts, :flags, 0)
    validate_flags!(flags)

    if byte_size(payload) > @max_u32 do
      raise ArgumentError,
            "payload exceeds u32 range: #{byte_size(payload)} bytes"
    end

    meta_bin = Metadata.encode(metadata)
    header_size = 10 + byte_size(meta_bin) + 4

    if header_size > @max_u16 do
      raise ArgumentError,
            "header_size exceeds u16 range: #{header_size} bytes (likely oversized metadata extension)"
    end

    frame_without_crc =
      <<
        @magic::binary,
        @version::unsigned-8,
        metadata.sketch_family::unsigned-8,
        metadata.sketch_family_version::unsigned-8,
        flags::unsigned-8,
        header_size::unsigned-little-16,
        meta_bin::binary,
        byte_size(payload)::unsigned-little-32,
        payload::binary
      >>

    crc = CRC.crc32c(frame_without_crc)
    <<frame_without_crc::binary, crc::unsigned-little-32>>
  end

  @doc """
  Decodes an EXSK v2 frame.

  Returns `{:ok, frame_map}` on success, or `{:error, %DeserializationError{}}`
  on failure. Never crashes the BEAM on malformed input — every parse
  pathway returns a structured error.

  The CRC32C is verified against the recomputed checksum over all bytes
  preceding it; any mismatch is reported with reason `"checksum mismatch ..."`.

  This decoder ONLY handles v2 frames. v1 frames are dispatched by
  `ExDataSketch.Binary.decode/1`, which sniffs the version byte and
  routes through the legacy `ExDataSketch.Codec`.

  ## Examples

      iex> meta = ExDataSketch.Hash.Metadata.new(:xxhash3, 0, 1, 1, :rust)
      iex> bin = ExDataSketch.Binary.Header.encode(meta, <<1, 2, 3>>)
      iex> {:ok, frame} = ExDataSketch.Binary.Header.decode(bin)
      iex> frame.payload
      <<1, 2, 3>>
      iex> frame.serialization_version
      2

  """
  @spec decode(binary()) :: {:ok, frame()} | {:error, Exception.t()}
  def decode(bin) when is_binary(bin) do
    with :ok <- check_minimum_size(bin),
         :ok <- check_magic_and_version(bin),
         {:ok, parts} <- parse_layout(bin),
         :ok <- verify_crc(parts, bin),
         :ok <- check_flags(parts),
         {:ok, metadata, <<>>} <- Metadata.decode(parts.meta_bin),
         :ok <- check_family_consistency(parts, metadata) do
      {:ok,
       %{
         magic: @magic,
         serialization_version: @version,
         sketch_family: parts.family,
         family_version: parts.family_version,
         flags: parts.flags,
         metadata: metadata,
         payload: parts.payload
       }}
    end
  end

  # -- decode pipeline --

  defp check_minimum_size(bin) when byte_size(bin) < 32 do
    {:error, DeserializationError.exception(reason: "frame too short for EXSK v2 header")}
  end

  defp check_minimum_size(_bin), do: :ok

  defp check_magic_and_version(<<@magic, @version::unsigned-8, _rest::binary>>), do: :ok

  defp check_magic_and_version(<<@magic, other::unsigned-8, _rest::binary>>) do
    {:error,
     DeserializationError.exception(reason: "unsupported EXSK frame version #{other}, expected 2")}
  end

  defp check_magic_and_version(<<_other::binary-size(4), _rest::binary>>) do
    {:error, DeserializationError.exception(reason: "invalid magic bytes, expected EXSK")}
  end

  defp check_magic_and_version(_),
    do: {:error, DeserializationError.exception(reason: "truncated EXSK frame")}

  defp parse_layout(bin) do
    <<
      @magic,
      @version::unsigned-8,
      family::unsigned-8,
      family_version::unsigned-8,
      flags::unsigned-8,
      header_size::unsigned-little-16,
      rest::binary
    >> = bin

    meta_len = header_size - 10 - 4

    cond do
      meta_len < 16 ->
        {:error,
         DeserializationError.exception(
           reason: "declared header_size #{header_size} is too small (need ≥ 30)"
         )}

      byte_size(rest) < meta_len + 4 + 4 ->
        {:error,
         DeserializationError.exception(
           reason: "frame truncated: declared header_size #{header_size} exceeds remaining bytes"
         )}

      true ->
        <<meta_bin::binary-size(^meta_len), payload_size::unsigned-little-32, tail::binary>> =
          rest

        cond do
          byte_size(tail) < payload_size + 4 ->
            {:error,
             DeserializationError.exception(
               reason:
                 "frame truncated: declared payload_size #{payload_size} exceeds remaining bytes"
             )}

          byte_size(tail) > payload_size + 4 ->
            {:error,
             DeserializationError.exception(
               reason:
                 "trailing bytes after payload: declared payload_size #{payload_size}, " <>
                   "#{byte_size(tail) - 4 - payload_size} extra"
             )}

          true ->
            <<payload::binary-size(^payload_size), crc_declared::unsigned-little-32>> = tail

            {:ok,
             %{
               family: family,
               family_version: family_version,
               flags: flags,
               header_size: header_size,
               meta_bin: meta_bin,
               payload_size: payload_size,
               payload: payload,
               crc_declared: crc_declared
             }}
        end
    end
  end

  defp verify_crc(parts, bin) do
    crc_off = byte_size(bin) - 4
    <<crc_input::binary-size(^crc_off), _crc::unsigned-little-32>> = bin
    actual = CRC.crc32c(crc_input)

    if actual == parts.crc_declared do
      :ok
    else
      {:error,
       DeserializationError.exception(
         reason:
           "checksum mismatch: declared 0x" <>
             Integer.to_string(parts.crc_declared, 16) <>
             ", computed 0x" <> Integer.to_string(actual, 16)
       )}
    end
  end

  defp check_family_consistency(parts, metadata) do
    cond do
      parts.family != metadata.sketch_family ->
        {:error,
         DeserializationError.exception(
           reason:
             "frame sketch_family (#{parts.family}) disagrees with metadata sketch_family " <>
               "(#{metadata.sketch_family})"
         )}

      parts.family_version != metadata.sketch_family_version ->
        {:error,
         DeserializationError.exception(
           reason:
             "frame family_version (#{parts.family_version}) disagrees with metadata " <>
               "sketch_family_version (#{metadata.sketch_family_version})"
         )}

      true ->
        :ok
    end
  end

  defp check_flags(%{flags: 0}), do: :ok

  defp check_flags(%{flags: flags}) do
    {:error,
     DeserializationError.exception(
       reason: "unsupported EXSK v2 flags 0x#{Integer.to_string(flags, 16)}"
     )}
  end

  defp validate_flags!(flags) when is_integer(flags) and flags >= 0 and flags <= 255, do: :ok

  defp validate_flags!(other) do
    raise ArgumentError, "flags must be a u8, got: #{inspect(other)}"
  end
end
