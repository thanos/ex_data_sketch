defmodule ExDataSketch.Binary.CRC do
  @moduledoc """
  CRC32C (Castagnoli polynomial, reflected, init 0xFFFFFFFF, xor-out
  0xFFFFFFFF) — the checksum used by EXSK v2 frames.

  CRC32C is the same checksum used by iSCSI, Btrfs, SCTP, and many storage
  systems. It is hardware-accelerated by Intel SSE 4.2 (`CRC32` instruction)
  and ARMv8.1+ (`CRC32CB/CRC32CH/CRC32CW/CRC32CX`), so the Rust NIF version
  is essentially free on modern hardware.

  ## Polynomial

      Reversed polynomial: 0x82F63B78 (Castagnoli reflected)
      Init value:          0xFFFFFFFF
      Final xor:           0xFFFFFFFF
      Reflect input/output: true

  ## Stability

  The CRC32C function is a well-specified standard. Output is identical
  across every implementation. The pure Elixir implementation and the
  Rust NIF implementation are property-tested for byte-identical agreement
  on random inputs.

  ## Examples

      iex> ExDataSketch.Binary.CRC.crc32c(<<>>)
      0

      iex> ExDataSketch.Binary.CRC.crc32c("123456789")
      0xE3069283

  """

  import Bitwise

  alias ExDataSketch.Hash
  alias ExDataSketch.Nif

  @poly_reflected 0x82F63B78
  @init 0xFFFFFFFF
  @final_xor 0xFFFFFFFF
  @mask32 0xFFFFFFFF

  # Build the 256-entry lookup table at compile time.
  @table (for byte <- 0..255 do
            Enum.reduce(0..7, byte, fn _, acc ->
              if Bitwise.band(acc, 1) == 1 do
                Bitwise.bxor(Bitwise.bsr(acc, 1), @poly_reflected)
              else
                Bitwise.bsr(acc, 1)
              end
            end)
          end)
         |> List.to_tuple()

  @doc """
  Computes the CRC32C of the given binary.

  Dispatches to the Rust NIF when available, falling back to a pure-Elixir
  table-driven implementation otherwise. Both implementations produce
  byte-identical output.

  ## Examples

      iex> ExDataSketch.Binary.CRC.crc32c(<<>>)
      0

      iex> ExDataSketch.Binary.CRC.crc32c("hello")
      0x9A71BB4C

  """
  @spec crc32c(binary()) :: non_neg_integer()
  def crc32c(bin) when is_binary(bin) do
    if Hash.nif_available?() do
      try do
        Nif.crc32c_nif(bin)
      rescue
        UndefinedFunctionError -> pure_crc32c(bin)
        ErlangError -> pure_crc32c(bin)
      end
    else
      pure_crc32c(bin)
    end
  end

  @doc """
  Pure-Elixir CRC32C implementation. Never calls the NIF.

  Provided for parity testing and as the BEAM-only fallback.

  ## Examples

      iex> ExDataSketch.Binary.CRC.pure_crc32c("123456789")
      0xE3069283

  """
  @spec pure_crc32c(binary()) :: non_neg_integer()
  def pure_crc32c(bin) when is_binary(bin) do
    bxor(reduce_crc(bin, @init), @final_xor) &&& @mask32
  end

  @doc """
  Returns the standard CRC32C check vector ("123456789" -> 0xE3069283).

  This vector is published by the CRC reference catalogue and matches every
  CRC32C implementation (iSCSI, Btrfs, SCTP, hardware CRC32 instruction).

  ## Examples

      iex> ExDataSketch.Binary.CRC.check_vector()
      {"123456789", 0xE3069283}

  """
  @spec check_vector() :: {binary(), non_neg_integer()}
  def check_vector, do: {"123456789", 0xE3069283}

  # -- pure implementation --

  defp reduce_crc(<<>>, acc), do: acc

  defp reduce_crc(<<byte, rest::binary>>, acc) do
    idx = bxor(acc, byte) &&& 0xFF
    table_val = elem(@table, idx)
    reduce_crc(rest, bxor(bsr(acc, 8), table_val))
  end
end
