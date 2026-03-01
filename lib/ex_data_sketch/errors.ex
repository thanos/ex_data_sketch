defmodule ExDataSketch.Errors do
  @moduledoc """
  Error types for ExDataSketch.

  All recoverable errors are represented as structs implementing the Exception
  behaviour. Functions that validate external input return tagged tuples:
  `{:ok, result} | {:error, %ErrorType{}}`.

  ## Error Types

  - `NotImplementedError` -- operation is stubbed and not yet implemented.
  - `InvalidOptionError` -- an option value is out of range or of wrong type.
  - `DeserializationError` -- binary data could not be decoded.
  - `IncompatibleSketchesError` -- sketches cannot be merged due to parameter mismatch.
  """

  defmodule NotImplementedError do
    @moduledoc """
    Raised when a stubbed function is called before its implementation is available.
    """
    defexception [:message]

    @impl true
    def exception(opts) do
      function = Keyword.get(opts, :function, "unknown")
      module = Keyword.get(opts, :module, "unknown")
      %__MODULE__{message: "#{module}.#{function} is not yet implemented"}
    end
  end

  defmodule InvalidOptionError do
    @moduledoc """
    Returned when an option value is invalid.
    """
    defexception [:message, :option, :value]

    @impl true
    def exception(opts) do
      option = Keyword.get(opts, :option)
      value = Keyword.get(opts, :value)

      message =
        Keyword.get(
          opts,
          :message,
          "invalid value #{inspect(value)} for option #{inspect(option)}"
        )

      %__MODULE__{message: message, option: option, value: value}
    end
  end

  defmodule DeserializationError do
    @moduledoc """
    Returned when binary data cannot be deserialized.
    """
    defexception [:message]

    @impl true
    def exception(opts) do
      reason = Keyword.get(opts, :reason, "unknown")
      %__MODULE__{message: "deserialization failed: #{reason}"}
    end
  end

  defmodule IncompatibleSketchesError do
    @moduledoc """
    Returned when attempting to merge sketches with incompatible parameters.
    """
    defexception [:message]

    @impl true
    def exception(opts) do
      reason = Keyword.get(opts, :reason, "parameter mismatch")
      %__MODULE__{message: "cannot merge sketches: #{reason}"}
    end
  end

  @doc """
  Wraps a value in an ok tuple.

  ## Examples

      iex> ExDataSketch.Errors.ok(:value)
      {:ok, :value}

  """
  @spec ok(term()) :: {:ok, term()}
  def ok(value), do: {:ok, value}

  @doc """
  Wraps an error struct in an error tuple.

  ## Examples

      iex> ExDataSketch.Errors.error(%ExDataSketch.Errors.InvalidOptionError{message: "bad"})
      {:error, %ExDataSketch.Errors.InvalidOptionError{message: "bad"}}

  """
  @spec error(Exception.t()) :: {:error, Exception.t()}
  def error(%_{} = err), do: {:error, err}

  @doc """
  Raises a NotImplementedError for the given module and function name.

  ## Examples

      iex> try do
      ...>   ExDataSketch.Errors.not_implemented!(ExDataSketch.HLL, "estimate")
      ...> rescue
      ...>   e in ExDataSketch.Errors.NotImplementedError -> e.message
      ...> end
      "ExDataSketch.HLL.estimate is not yet implemented"

  """
  @spec not_implemented!(module(), String.t()) :: no_return()
  def not_implemented!(module, function) do
    raise NotImplementedError, module: inspect(module), function: function
  end
end
