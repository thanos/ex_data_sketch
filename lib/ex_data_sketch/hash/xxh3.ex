defmodule ExDataSketch.Hash.XXH3 do
  @moduledoc """
  XXHash3 (64-bit) hash algorithm.

  XXHash3 is the **default** hash algorithm for ExDataSketch. It is stable
  across platforms, OS versions, CPU architectures, and OTP versions when
  computed via the Rust NIF.

  ## Properties

  | Property            | Value                                   |
  |---------------------|-----------------------------------------|
  | Output bits         | 64                                      |
  | Seedable            | Yes (`u64`)                             |
  | Cross-platform      | Yes                                     |
  | Cross-OTP stable    | Yes (via NIF). No when NIF unavailable. |
  | BEAM-side fallback  | `:erlang.phash2` + `mix64`              |

  When the Rust NIF is available, `hash/2` calls the upstream `xxhash-rust`
  implementation directly. When the NIF is unavailable, `hash/2` raises an
  `ArgumentError` so that hash drift cannot occur silently — callers that
  want a NIF-less fallback must explicitly select `ExDataSketch.Hash`
  with `hash_strategy: :phash2`.

  ## Examples

      iex> ExDataSketch.Hash.XXH3.hash("hello") == ExDataSketch.Hash.XXH3.hash("hello")
      true

      iex> ExDataSketch.Hash.XXH3.hash("hello", 1) != ExDataSketch.Hash.XXH3.hash("hello", 2)
      true

  """

  import Bitwise

  alias ExDataSketch.Hash
  alias ExDataSketch.Nif

  @mask64 0xFFFFFFFFFFFFFFFF

  @doc """
  Returns the algorithm identifier `:xxhash3`.

  ## Examples

      iex> ExDataSketch.Hash.XXH3.id()
      :xxhash3

  """
  @spec id() :: :xxhash3
  def id, do: :xxhash3

  @doc """
  Hashes a binary using XXHash3 (64-bit) with the given seed.

  Requires the Rust NIF. Raises `ArgumentError` if the NIF is not loaded.

  ## Examples

      iex> h = ExDataSketch.Hash.XXH3.hash("hello", 0)
      iex> is_integer(h) and h >= 0 and h <= 0xFFFFFFFFFFFFFFFF
      true

  """
  @spec hash(binary(), non_neg_integer()) :: Hash.hash64()
  def hash(data, seed \\ 0) when is_binary(data) and is_integer(seed) and seed >= 0 do
    if Hash.nif_available?() do
      Nif.xxhash3_64_seeded_nif(data, seed &&& @mask64)
    else
      raise ArgumentError,
            "ExDataSketch.Hash.XXH3.hash/2 requires the Rust NIF but it is not available. " <>
              "Set hash_strategy: :phash2 to use the BEAM-only fallback (non-portable across OTP majors)."
    end
  end

  @doc """
  Returns whether this algorithm is available in the current runtime.

  XXHash3 requires the Rust NIF to be loaded.

  ## Examples

      iex> is_boolean(ExDataSketch.Hash.XXH3.available?())
      true

  """
  @spec available?() :: boolean()
  def available?, do: Hash.nif_available?()
end
