# Roadmap consistency check for CI.
#
# Guards against shipping a release whose README/roadmap docs drifted
# from what mix.exs actually declares as the current version. Twice
# deferred (v0.8.0 carry-forward X-R2, v0.9.0 carry-forward), landed in
# v0.10.0 Phase 8.
#
# Always checks that README.md's roadmap table has a row for the current
# base version (mix.exs's @version with any "-dev" suffix stripped) --
# this alone catches "shipped a phase but forgot the roadmap row"
# regardless of dev/release state, so it can't be skipped by staying on
# a "-dev" version forever.
#
# Only when @version has NO "-dev" suffix (i.e. this commit claims to be
# an actual release) does it also assert: that row's status is
# "Released", the README install snippet matches the same version, and
# guides/roadmap.md has moved on to previewing the next release (its
# title no longer contains the just-shipped version). This avoids
# false-positives on ordinary dev-cycle commits before the release
# commit itself lands.

defmodule CI.CheckRoadmap do
  @mix_exs "mix.exs"
  @readme "README.md"
  @roadmap_guide "guides/roadmap.md"

  def run do
    version = read_mix_version()
    dev? = String.ends_with?(version, "-dev")
    base_version = String.replace_suffix(version, "-dev", "")

    readme = File.read!(@readme)

    checks =
      [check_roadmap_row_exists(readme, base_version)] ++
        if dev? do
          []
        else
          [
            check_roadmap_row_released(readme, base_version),
            check_install_snippet(readme, base_version),
            check_next_roadmap_preview(base_version)
          ]
        end

    failures = Enum.filter(checks, &(&1 != :ok))

    if failures == [] do
      IO.puts("PASS: roadmap docs consistent with mix.exs version #{version}")
    else
      Enum.each(failures, fn {:error, msg} -> IO.puts("FAIL: #{msg}") end)
      System.halt(1)
    end
  end

  defp read_mix_version do
    content = File.read!(@mix_exs)

    case Regex.run(~r/@version\s+"([^"]+)"/, content) do
      [_, version] ->
        version

      nil ->
        IO.puts("FAIL: could not find @version in #{@mix_exs}")
        System.halt(1)
    end
  end

  defp roadmap_row_regex(base_version) do
    Regex.compile!("\\|\\s*v#{Regex.escape(base_version)}\\s*\\|(.*)\\|\\s*(\\w+)\\s*\\|")
  end

  defp check_roadmap_row_exists(readme, base_version) do
    if Regex.match?(roadmap_row_regex(base_version), readme) do
      :ok
    else
      {:error,
       "#{@readme}'s roadmap table has no row for v#{base_version} " <>
         "(add one when a phase ships, even before the release commit)"}
    end
  end

  defp check_roadmap_row_released(readme, base_version) do
    case Regex.run(roadmap_row_regex(base_version), readme) do
      [_, _desc, status] when status == "Released" ->
        :ok

      [_, _desc, status] ->
        {:error,
         "#{@readme}'s roadmap row for v#{base_version} is marked \"#{status}\", " <>
           "expected \"Released\" since mix.exs is no longer -dev"}

      nil ->
        {:error, "#{@readme}'s roadmap table has no row for v#{base_version}"}
    end
  end

  defp check_install_snippet(readme, base_version) do
    case Regex.run(~r/ex_data_sketch,\s*"~>\s*([\d.]+)"/, readme) do
      [_, snippet_version] when snippet_version == base_version ->
        :ok

      [_, snippet_version] ->
        {:error,
         "#{@readme}'s install snippet says \"~> #{snippet_version}\", " <>
           "expected \"~> #{base_version}\""}

      nil ->
        {:error, "#{@readme} has no `{:ex_data_sketch, \"~> X.Y.Z\"}` install snippet"}
    end
  end

  defp check_next_roadmap_preview(base_version) do
    case File.read(@roadmap_guide) do
      {:ok, content} ->
        [title | _] = String.split(content, "\n", parts: 2)

        if String.contains?(title, "v#{base_version}") do
          {:error,
           "#{@roadmap_guide} is still titled for the just-released v#{base_version} " <>
             "(rewrite it as the next release's preview stub)"}
        else
          :ok
        end

      {:error, _} ->
        {:error, "#{@roadmap_guide} not found"}
    end
  end
end

CI.CheckRoadmap.run()
