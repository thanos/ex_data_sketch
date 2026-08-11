# Persistence Overhead Benchmark
#
# Run with: MIX_ENV=dev mix run bench/persistence_bench.exs

alias ExDataSketch.{HLL, CMS, Storage.ETS, Storage.DETS}

IO.puts("ExDataSketch Persistence Benchmark")
IO.puts("====================================")
IO.puts("Elixir: #{System.version()}")
IO.puts("OTP: #{System.otp_release()}")
IO.puts("Schedulers: #{System.schedulers_online()}")
IO.puts("")

sizes = [100, 1_000, 10_000]

for size <- sizes do
  items = for i <- 0..(size - 1), do: "item_#{i}"
  hll = HLL.from_enumerable(items, p: 14)
  cms = CMS.from_enumerable(items, width: 128, depth: 5)

  IO.puts("")
  IO.puts("--- HLL persistence overhead (#{size} items) ---")

  Benchee.run(
    %{
      "ETS save" => fn ->
        table = :"bench_ets_hll_#{System.unique_integer([:positive])}"
        :ets.new(table, [:set, :public, :named_table])
        ETS.save(hll, table, "bench_hll")
        :ets.delete(table)
      end,
      "ETS load" => fn ->
        table = :"bench_ets_hll_ld_#{System.unique_integer([:positive])}"
        :ets.new(table, [:set, :public, :named_table])
        ETS.save(hll, table, "bench_hll")
        {:ok, _} = ETS.load(HLL, table, "bench_hll")
        :ets.delete(table)
      end,
      "ETS merge" => fn ->
        table = :"bench_ets_hll_mg_#{System.unique_integer([:positive])}"
        :ets.new(table, [:set, :public, :named_table])
        ETS.save(hll, table, "bench_hll")
        ETS.merge(HLL.new(p: 14) |> HLL.update("extra"), table, "bench_hll")
        :ets.delete(table)
      end
    },
    time: 2,
    memory_time: 1,
    print: [configuration: false, benchmarking: false]
  )
end

IO.puts("")

for size <- sizes do
  items = for i <- 0..(size - 1), do: "item_#{i}"
  cms = CMS.from_enumerable(items, width: 128, depth: 5)

  IO.puts("--- CMS persistence overhead (#{size} items) ---")

  Benchee.run(
    %{
      "ETS save CMS" => fn ->
        table = :"bench_ets_cms_#{System.unique_integer([:positive])}"
        :ets.new(table, [:set, :public, :named_table])
        ETS.save(cms, table, "bench_cms")
        :ets.delete(table)
      end
    },
    time: 2,
    memory_time: 1,
    print: [configuration: false, benchmarking: false]
  )
end

IO.puts("")
IO.puts("Persistence benchmark complete.")
