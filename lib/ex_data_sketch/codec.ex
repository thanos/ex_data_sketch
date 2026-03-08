defmodule ExDataSketch.Codec do
  @moduledoc """
  ExDataSketch-native binary serialization codec (EXSK format).

  Provides a stable binary format for serializing and deserializing sketch state.
  This format is internal to ExDataSketch and is not compatible with Apache
  DataSketches. For cross-language interop, use the `serialize_datasketches/1`
  and `deserialize_datasketches/1` functions on individual sketch modules.

  ## Binary Layout

  All multi-byte integers are little-endian.

      Offset  Size    Field
      ------  ------  -----
      0       4       Magic bytes: "EXSK" (0x45 0x58 0x53 0x4B)
      4       1       Format version (u8, currently 1)
      5       1       Sketch ID (u8, see Sketch IDs below)
      6       4       Params length (u32 little-endian)
      10      N       Params binary (sketch-specific parameters)
      10+N    4       State length (u32 little-endian)
      14+N    M       State binary (raw sketch state)

  Total: 14 + N + M bytes.

  ## Sketch IDs

  - 1: HLL (HyperLogLog)
  - 2: CMS (Count-Min Sketch)
  - 3: Theta
  - 4: KLL (Quantiles)
  - 5: DDSketch (Quantiles)
  - 6: FrequentItems (SpaceSaving)
  - 7: Bloom
  - 8: Cuckoo
  - 9: Quotient
  - 10: CQF (Counting Quotient Filter)
  - 11: XorFilter

  ## Versioning

  The format version byte allows forward-compatible changes. Decoders must
  reject versions they do not support with a clear error message.
  """

  alias ExDataSketch.Errors.DeserializationError

  @magic "EXSK"
  @current_version 1

  @sketch_id_hll 1
  @sketch_id_cms 2
  @sketch_id_theta 3
  @sketch_id_kll 4
  @sketch_id_ddsketch 5
  @sketch_id_fi 6
  @sketch_id_bloom 7
  @sketch_id_cuckoo 8
  @sketch_id_quotient 9
  @sketch_id_cqf 10
  @sketch_id_xor 11
  @sketch_id_iblt 12

  @type sketch_id :: 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12
  @type decoded :: %{
          version: pos_integer(),
          sketch_id: sketch_id(),
          params: binary(),
          state: binary()
        }

  @doc """
  Returns the magic bytes used in the EXSK format header.

  ## Examples

      iex> ExDataSketch.Codec.magic()
      "EXSK"

  """
  @spec magic() :: binary()
  def magic, do: @magic

  @doc """
  Returns the current format version.

  ## Examples

      iex> ExDataSketch.Codec.version()
      1

  """
  @spec version() :: pos_integer()
  def version, do: @current_version

  @doc """
  Returns the sketch ID constant for HLL.

  ## Examples

      iex> ExDataSketch.Codec.sketch_id_hll()
      1

  """
  @spec sketch_id_hll() :: sketch_id()
  def sketch_id_hll, do: @sketch_id_hll

  @doc """
  Returns the sketch ID constant for CMS.

  ## Examples

      iex> ExDataSketch.Codec.sketch_id_cms()
      2

  """
  @spec sketch_id_cms() :: sketch_id()
  def sketch_id_cms, do: @sketch_id_cms

  @doc """
  Returns the sketch ID constant for Theta.

  ## Examples

      iex> ExDataSketch.Codec.sketch_id_theta()
      3

  """
  @spec sketch_id_theta() :: sketch_id()
  def sketch_id_theta, do: @sketch_id_theta

  @doc """
  Returns the sketch ID constant for KLL.

  ## Examples

      iex> ExDataSketch.Codec.sketch_id_kll()
      4

  """
  @spec sketch_id_kll() :: sketch_id()
  def sketch_id_kll, do: @sketch_id_kll

  @doc """
  Returns the sketch ID constant for DDSketch.

  ## Examples

      iex> ExDataSketch.Codec.sketch_id_ddsketch()
      5

  """
  @spec sketch_id_ddsketch() :: sketch_id()
  def sketch_id_ddsketch, do: @sketch_id_ddsketch

  @doc """
  Returns the sketch ID constant for FrequentItems.

  ## Examples

      iex> ExDataSketch.Codec.sketch_id_fi()
      6

  """
  @spec sketch_id_fi() :: sketch_id()
  def sketch_id_fi, do: @sketch_id_fi

  @doc """
  Returns the sketch ID constant for Bloom.

  ## Examples

      iex> ExDataSketch.Codec.sketch_id_bloom()
      7

  """
  @spec sketch_id_bloom() :: sketch_id()
  def sketch_id_bloom, do: @sketch_id_bloom

  @doc """
  Returns the sketch ID constant for Cuckoo.

  ## Examples

      iex> ExDataSketch.Codec.sketch_id_cuckoo()
      8

  """
  @spec sketch_id_cuckoo() :: sketch_id()
  def sketch_id_cuckoo, do: @sketch_id_cuckoo

  @doc """
  Returns the sketch ID constant for Quotient.

  ## Examples

      iex> ExDataSketch.Codec.sketch_id_quotient()
      9

  """
  @spec sketch_id_quotient() :: sketch_id()
  def sketch_id_quotient, do: @sketch_id_quotient

  @doc """
  Returns the sketch ID constant for CQF.

  ## Examples

      iex> ExDataSketch.Codec.sketch_id_cqf()
      10

  """
  @spec sketch_id_cqf() :: sketch_id()
  def sketch_id_cqf, do: @sketch_id_cqf

  @doc """
  Returns the sketch ID constant for XorFilter.

  ## Examples

      iex> ExDataSketch.Codec.sketch_id_xor()
      11

  """
  @spec sketch_id_xor() :: sketch_id()
  def sketch_id_xor, do: @sketch_id_xor

  @doc """
  Returns the sketch ID constant for IBLT.

  ## Examples

      iex> ExDataSketch.Codec.sketch_id_iblt()
      12

  """
  @spec sketch_id_iblt() :: sketch_id()
  def sketch_id_iblt, do: @sketch_id_iblt

  @doc """
  Encodes sketch data into the EXSK binary format.

  ## Parameters

  - `sketch_id` - sketch type identifier (1=HLL, 2=CMS, 3=Theta, 4=KLL, 5=DDSketch, 6=FrequentItems, 7=Bloom, 8=Cuckoo, 9=Quotient, 10=CQF, 11=XorFilter, 12=IBLT)
  - `version` - format version (use `Codec.version/0` for current)
  - `params_bin` - binary-encoded sketch parameters
  - `state_bin` - raw sketch state binary

  ## Examples

      iex> bin = ExDataSketch.Codec.encode(1, 1, <<14>>, <<0, 0, 0>>)
      iex> <<"EXSK", 1, 1, _rest::binary>> = bin
      iex> byte_size(bin)
      18

  """
  @spec encode(sketch_id(), pos_integer(), binary(), binary()) :: binary()
  def encode(sketch_id, version, params_bin, state_bin)
      when is_integer(sketch_id) and is_integer(version) and
             is_binary(params_bin) and is_binary(state_bin) do
    params_len = byte_size(params_bin)
    state_len = byte_size(state_bin)

    <<
      @magic::binary,
      version::unsigned-8,
      sketch_id::unsigned-8,
      params_len::unsigned-little-32,
      params_bin::binary,
      state_len::unsigned-little-32,
      state_bin::binary
    >>
  end

  @doc """
  Decodes an EXSK binary into its components.

  Returns `{:ok, map}` on success or `{:error, %DeserializationError{}}` on failure.
  The returned map contains `:version`, `:sketch_id`, `:params`, and `:state`.

  ## Examples

      iex> bin = ExDataSketch.Codec.encode(1, 1, <<14>>, <<0, 0>>)
      iex> {:ok, decoded} = ExDataSketch.Codec.decode(bin)
      iex> decoded.sketch_id
      1
      iex> decoded.params
      <<14>>
      iex> decoded.state
      <<0, 0>>

      iex> ExDataSketch.Codec.decode(<<"BAAD", 1, 1, 0::32, 0::32>>)
      {:error, %ExDataSketch.Errors.DeserializationError{message: "deserialization failed: invalid magic bytes, expected EXSK"}}

      iex> ExDataSketch.Codec.decode(<<1, 2>>)
      {:error, %ExDataSketch.Errors.DeserializationError{message: "deserialization failed: binary too short for EXSK header"}}

  """
  @spec decode(binary()) :: {:ok, decoded()} | {:error, Exception.t()}
  def decode(<<@magic::binary, version::unsigned-8, sketch_id::unsigned-8, rest::binary>>) do
    decode_body(version, sketch_id, rest)
  end

  def decode(<<_other::binary-size(4), _::binary>>) do
    {:error, DeserializationError.exception(reason: "invalid magic bytes, expected EXSK")}
  end

  def decode(_binary) do
    {:error, DeserializationError.exception(reason: "binary too short for EXSK header")}
  end

  defp decode_body(version, _sketch_id, _rest) when version > @current_version do
    {:error,
     DeserializationError.exception(
       reason: "unsupported version #{version}, max supported: #{@current_version}"
     )}
  end

  defp decode_body(version, sketch_id, <<params_len::unsigned-little-32, rest::binary>>) do
    case rest do
      <<params::binary-size(^params_len), state_len::unsigned-little-32, rest2::binary>> ->
        case rest2 do
          <<state::binary-size(^state_len)>> ->
            {:ok,
             %{
               version: version,
               sketch_id: sketch_id,
               params: params,
               state: state
             }}

          <<_state::binary-size(^state_len), _trailing::binary>> ->
            {:error, DeserializationError.exception(reason: "trailing bytes after state segment")}

          _ ->
            {:error,
             DeserializationError.exception(
               reason: "state segment shorter than declared length #{state_len}"
             )}
        end

      _ ->
        {:error,
         DeserializationError.exception(
           reason: "params segment shorter than declared length #{params_len}"
         )}
    end
  end

  defp decode_body(_version, _sketch_id, _rest) do
    {:error, DeserializationError.exception(reason: "truncated header after sketch ID")}
  end
end
