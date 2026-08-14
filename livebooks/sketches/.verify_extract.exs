file = System.argv() |> List.first()
content = File.read!(file)

# Extract all ```elixir ... ``` fenced code blocks, skipping the first
# (the Mix.install cell -- deps are already loaded via the real project).
blocks =
  Regex.scan(~r/```elixir\n(.*?)\n```/s, content, capture: :all_but_first)
  |> Enum.map(fn [code] -> code end)

[mix_install_src | cells] = blocks

# The Mix.install cell itself is skipped (deps are already loaded via the
# real project), but its `config:` option (e.g. `config: [ex_data_sketch:
# [backend: ExDataSketch.Backend.Rust]]`) is NOT -- apply it via
# Application.put_env/3 before evaluating anything else, so a notebook
# that configures a non-default backend (or any other app env) actually
# gets tested under that configuration, matching what a real Livebook
# session running the Mix.install cell would do. Parsed via the AST
# (not string-matched) so it's robust to formatting.
config_kw =
  with {:ok, quoted} <- Code.string_to_quoted(mix_install_src),
       {{:., _, [{:__aliases__, _, [:Mix]}, :install]}, _, [_deps, opts]} <- quoted,
       true <- Keyword.keyword?(opts),
       {:ok, config_ast} <- Keyword.fetch(opts, :config) do
    {config, _bindings} = Code.eval_quoted(config_ast)
    config
  else
    _ -> []
  end

Enum.each(config_kw, fn {app, app_config} ->
  Enum.each(app_config, fn {key, val} -> Application.put_env(app, key, val) end)
end)

if config_kw != [] do
  IO.puts("Applied Mix.install config: #{inspect(config_kw)}")
end

# Code.eval_string/3 doesn't carry aliases between separate calls (only
# bindings), so collect every `alias ...` line used anywhere in the file
# and prepend it to each cell -- redundant but harmless.
aliases =
  Regex.scan(~r/^\s*alias\s+.+$/m, content)
  |> Enum.map(fn [line] -> line end)
  |> Enum.uniq()
  |> Enum.join("\n")

IO.puts("=== #{Path.basename(file)}: #{length(cells)} code cells (after Mix.install) ===")

Enum.with_index(cells, 1)
|> Enum.reduce([], fn {code, idx}, bindings ->
  try do
    {_result, new_bindings} = Code.eval_string(aliases <> "\n" <> code, bindings)
    IO.puts("  cell #{idx}: OK")
    new_bindings
  rescue
    e ->
      IO.puts("  cell #{idx}: FAILED")
      IO.puts("    #{Exception.format(:error, e, __STACKTRACE__)}")
      IO.puts("    --- code ---")
      IO.puts("    " <> String.replace(code, "\n", "\n    "))
      bindings
  end
end)
