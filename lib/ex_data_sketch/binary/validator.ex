defmodule ExDataSketch.Binary.Validator do
  @moduledoc """
  Structured validation primitives for EXSK frames.

  This module exposes the individual checks that `ExDataSketch.Binary.Header.decode/1`
  performs as discrete, composable functions. Tests, fuzzers, and tools
  (e.g., a hypothetical "exsk inspect" CLI) can use these to surface
  precisely which check failed without parsing the full frame.

  All validators follow a uniform contract:

  - Return `:ok` on success.
  - Return `{:error, %ExDataSketch.Errors.DeserializationError{}}` on failure.
  - NEVER crash the BEAM.

  ## See also

  - `ExDataSketch.Binary.Header` — the main decode pipeline.
  - `ExDataSketch.Binary.CRC` — checksum algorithm.
  - `plans/corruption_detection.md` — full error taxonomy.
  """

  alias ExDataSketch.Binary.CRC
  alias ExDataSketch.Errors.DeserializationError

  @magic "EXSK"

  @doc """
  Returns `:ok` when the binary is at least the v2 minimum frame size.

  ## Examples

      iex> ExDataSketch.Binary.Validator.check_minimum_v2_size(:binary.copy(<<0>>, 32))
      :ok

      iex> match?({:error, _}, ExDataSketch.Binary.Validator.check_minimum_v2_size(<<1, 2, 3>>))
      true

  """
  @spec check_minimum_v2_size(binary()) :: :ok | {:error, Exception.t()}
  def check_minimum_v2_size(bin) when is_binary(bin) and byte_size(bin) >= 32, do: :ok

  def check_minimum_v2_size(_bin) do
    {:error,
     DeserializationError.exception(
       reason: "binary too short for EXSK v2 frame (minimum 32 bytes)"
     )}
  end

  @doc """
  Returns `:ok` when the leading 4 bytes are the EXSK magic.

  ## Examples

      iex> ExDataSketch.Binary.Validator.check_magic("EXSK" <> <<0, 0>>)
      :ok

      iex> match?({:error, _}, ExDataSketch.Binary.Validator.check_magic("BAAD"))
      true

  """
  @spec check_magic(binary()) :: :ok | {:error, Exception.t()}
  def check_magic(<<@magic, _rest::binary>>), do: :ok

  def check_magic(<<_other::binary-size(4), _rest::binary>>) do
    {:error, DeserializationError.exception(reason: "invalid magic bytes, expected EXSK")}
  end

  def check_magic(_bin) do
    {:error, DeserializationError.exception(reason: "binary too short to contain EXSK magic")}
  end

  @doc """
  Returns `:ok` when the frame is the expected version.

  ## Examples

      iex> ExDataSketch.Binary.Validator.check_version("EXSK" <> <<2>>, 2)
      :ok

      iex> match?({:error, _}, ExDataSketch.Binary.Validator.check_version("EXSK" <> <<1>>, 2))
      true

  """
  @spec check_version(binary(), non_neg_integer()) :: :ok | {:error, Exception.t()}
  def check_version(<<@magic, version::unsigned-8, _rest::binary>>, expected) do
    if version == expected do
      :ok
    else
      {:error,
       DeserializationError.exception(
         reason: "unsupported EXSK frame version #{version}, expected #{expected}"
       )}
    end
  end

  def check_version(_bin, _expected) do
    {:error, DeserializationError.exception(reason: "binary too short to contain version byte")}
  end

  @doc """
  Verifies the trailing CRC32C of a frame against the recomputed checksum.

  Accepts the *full* frame including the trailing 4-byte CRC32C. Returns
  `:ok` if the recomputed CRC over `bin[0 .. -5]` equals the declared
  trailer; an error otherwise.

  Useful in fuzz tests that want to assert a single bit-flip is detected.

  ## Examples

      iex> meta = ExDataSketch.Hash.Metadata.new(:xxhash3, 0, 1, 1, :rust)
      iex> frame = ExDataSketch.Binary.Header.encode(meta, <<1, 2, 3>>)
      iex> ExDataSketch.Binary.Validator.check_crc(frame)
      :ok

  """
  @spec check_crc(binary()) :: :ok | {:error, Exception.t()}
  def check_crc(bin) when is_binary(bin) and byte_size(bin) >= 4 do
    crc_off = byte_size(bin) - 4
    <<crc_input::binary-size(^crc_off), declared::unsigned-little-32>> = bin
    actual = CRC.crc32c(crc_input)

    if actual == declared do
      :ok
    else
      {:error,
       DeserializationError.exception(
         reason:
           "checksum mismatch: declared 0x" <>
             Integer.to_string(declared, 16) <>
             ", computed 0x" <> Integer.to_string(actual, 16)
       )}
    end
  end

  def check_crc(_bin) do
    {:error, DeserializationError.exception(reason: "binary too short to contain CRC32C trailer")}
  end
end
