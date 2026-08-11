file = System.argv() |> List.first()
content = File.read!(file)

# Extract all ```elixir ... ``` fenced code blocks, skipping the first
# (the Mix.install cell -- deps are already loaded via the real project).
blocks =
  Regex.scan(~r/```elixir\n(.*?)\n```/s, content, capture: :all_but_first)
  |> Enum.map(fn [code] -> code end)

[_mix_install | cells] = blocks

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
