defmodule ExDataSketch.Integration do
  @moduledoc """
  Runtime dependency detection for optional integrations.

  This module provides a unified interface for checking whether optional
  dependencies (Broadway, Flow, CubDB, Ecto) are available at runtime.
  It is used internally by integration modules to provide clear error
  messages when a dependency is missing.

  ## Configuration

  Optional dependencies can be explicitly enabled or disabled via
  application config:

      config :ex_data_sketch, :integrations,
        broadway: true,
        flow: true

  When not explicitly configured, availability defaults to whether the
  dependency is loaded at runtime.

  ## Supported Integrations

  | Integration  | Dependency        | Always available? |
  |-------------|-------------------|-------------------|
  | GenStage    | OTP (always)      | Yes               |
  | Broadway    | `:broadway`       | No                |
  | Flow        | `:flow`           | No                |
  | CubDB       | `:cubdb`          | No                |
  | Ecto        | `:ecto_sql`       | No                |
  | Mnesia      | OTP (always)      | Yes               |
  | ETS         | OTP (always)      | Yes               |
  | DETS        | OTP (always)      | Yes               |
  | OpenTelemetry | `:opentelemetry_api` | No            |
  """

  @broadway_available Code.ensure_loaded?(Broadway)
  @flow_available Code.ensure_loaded?(Flow)
  @cubdb_available Code.ensure_loaded?(CubDB)
  @ecto_available Code.ensure_loaded?(Ecto.Adapters.SQL)
  @opentelemetry_available Code.ensure_loaded?(OpenTelemetry)

  @doc """
  Returns whether the Broadway library is available.

  Checks compile-time availability and runtime configuration.

  ## Examples

      iex> is_boolean(ExDataSketch.Integration.broadway_available?())
      true
  """
  @spec broadway_available?() :: boolean()
  def broadway_available? do
    configured?(:broadway, @broadway_available)
  end

  @doc """
  Returns whether the Flow library is available.

  Checks compile-time availability and runtime configuration.

  ## Examples

      iex> is_boolean(ExDataSketch.Integration.flow_available?())
      true
  """
  @spec flow_available?() :: boolean()
  def flow_available? do
    configured?(:flow, @flow_available)
  end

  @doc """
  Raises an error if Broadway is not available.

  Provides a clear error message directing the user to add the dependency.

  ## Examples

      iex> ExDataSketch.Integration.require_broadway!()
      :ok

      # When Broadway is not available:
      # ** (RuntimeError) Broadway integration requires the :broadway dependency.
      # Add {:broadway, "~> 1.0"} to your mix.exs dependencies.
  """
  @spec require_broadway!() :: :ok
  def require_broadway! do
    if broadway_available?() do
      :ok
    else
      raise "Broadway integration requires the :broadway dependency. " <>
              "Add {:broadway, \"~> 1.0\"} to your mix.exs dependencies."
    end
  end

  @doc """
  Raises an error if Flow is not available.

  Provides a clear error message directing the user to add the dependency.

  ## Examples

      iex> ExDataSketch.Integration.require_flow!()
      :ok

      # When Flow is not available:
      # ** (RuntimeError) Flow integration requires the :flow dependency.
      # Add {:flow, "~> 1.2"} to your mix.exs dependencies.
  """
  @spec require_flow!() :: :ok
  def require_flow! do
    if flow_available?() do
      :ok
    else
      raise "Flow integration requires the :flow dependency. " <>
              "Add {:flow, \"~> 1.2\"} to your mix.exs dependencies."
    end
  end

  defp configured?(key, default) do
    case Application.get_env(:ex_data_sketch, :integrations, []) |> Keyword.get(key) do
      nil -> default
      true -> true
      false -> false
      other -> other
    end
  end

  @doc """
  Returns whether the OpenTelemetry API library is available.

  Checks compile-time availability and runtime configuration.

  ## Examples

      iex> is_boolean(ExDataSketch.Integration.opentelemetry_available?())
      true
  """
  @spec opentelemetry_available?() :: boolean()
  def opentelemetry_available? do
    configured?(:opentelemetry, @opentelemetry_available)
  end

  @doc """
  Raises an error if OpenTelemetry is not available.

  Provides a clear error message directing the user to add the dependency.

  ## Examples

      iex> ExDataSketch.Integration.require_opentelemetry!()
      :ok

      # When OpenTelemetry is not available:
      # ** (RuntimeError) OpenTelemetry integration requires the :opentelemetry_api dependency.
      # Add {:opentelemetry_api, "~> 1.0"} to your mix.exs dependencies.
  """
  @spec require_opentelemetry!() :: :ok
  def require_opentelemetry! do
    if opentelemetry_available?() do
      :ok
    else
      raise "OpenTelemetry integration requires the :opentelemetry_api dependency. " <>
              "Add {:opentelemetry_api, \"~> 1.0\"} to your mix.exs dependencies."
    end
  end

  @doc """
  Returns whether the CubDB library is available.

  Checks compile-time availability and runtime configuration.

  ## Examples

      iex> is_boolean(ExDataSketch.Integration.cubdb_available?())
      true
  """
  @spec cubdb_available?() :: boolean()
  def cubdb_available? do
    configured_with_backends?(:cubdb, @cubdb_available)
  end

  @doc """
  Returns whether the Ecto library is available.

  Checks compile-time availability and runtime configuration.

  ## Examples

      iex> is_boolean(ExDataSketch.Integration.ecto_available?())
      true
  """
  @spec ecto_available?() :: boolean()
  def ecto_available? do
    configured_with_backends?(:ecto, @ecto_available)
  end

  @doc """
  Raises an error if CubDB is not available.

  Provides a clear error message directing the user to add the dependency.

  ## Examples

      iex> ExDataSketch.Integration.require_cubdb!()
      :ok

      # When CubDB is not available:
      # ** (RuntimeError) CubDB persistence requires the :cubdb dependency.
      # Add {:cubdb, "~> 2.0"} to your mix.exs dependencies.
  """
  @spec require_cubdb!() :: :ok
  def require_cubdb! do
    if cubdb_available?() do
      :ok
    else
      raise "CubDB persistence requires the :cubdb dependency. " <>
              "Add {:cubdb, \"~> 2.0\"} to your mix.exs dependencies."
    end
  end

  @doc """
  Raises an error if Ecto is not available.

  Provides a clear error message directing the user to add the dependency.

  ## Examples

      iex> ExDataSketch.Integration.require_ecto!()
      :ok

      # When Ecto is not available:
      # ** (RuntimeError) Ecto persistence requires the :ecto_sql dependency.
      # Add {:ecto_sql, "~> 3.0"} to your mix.exs dependencies.
  """
  @spec require_ecto!() :: :ok
  def require_ecto! do
    if ecto_available?() do
      :ok
    else
      raise "Ecto persistence requires the :ecto_sql dependency. " <>
              "Add {:ecto_sql, \"~> 3.0\"} to your mix.exs dependencies."
    end
  end

  defp configured_with_backends?(key, default) do
    backends = Application.get_env(:ex_data_sketch, :persistence_backends, [])

    case Keyword.get(backends, key) do
      nil -> default
      config when is_list(config) -> Keyword.get(config, :enabled, default)
      true -> true
      false -> false
    end
  end
end
