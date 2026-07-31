defmodule Mix.Tasks.ExDataSketch.Gen.Migration do
  @moduledoc """
  Generates a migration for the `ex_data_sketch_sketches` table.

  ## Usage

      mix ex_data_sketch.gen.migration --repo MyApp.Repo

  This creates a migration file in your application's `priv/repo/migrations`
  directory. Run `mix ecto.migrate` to apply the migration.

  ## Options

  - `--repo` -- the Ecto repo module (required).
  """

  use Mix.Task

  @shortdoc "Generates an Ecto migration for the ex_data_sketch_sketches table"

  @impl Mix.Task
  def run(args) do
    {opts, _, _} = OptionParser.parse(args, switches: [repo: :string])

    repo =
      Keyword.get(opts, :repo) ||
        Mix.raise("Expected --repo to be given, for example: --repo MyApp.Repo")

    repo_module = String.to_atom("Elixir." <> repo)

    migration_dir = Path.join([priv_dir(repo_module), "migrations"])
    File.mkdir_p!(migration_dir)

    timestamp = timestamp()
    migration_file = Path.join(migration_dir, "#{timestamp}_add_ex_data_sketch_sketches.exs")

    migration_content = """
    defmodule #{inspect(repo_module)}.Migrations.AddExDataSketchSketches do
      use Ecto.Migration

      def up do
        Enum.each(ExDataSketch.Storage.Ecto.Migration.up_commands(), &execute/1)
      end

      def down do
        Enum.each(ExDataSketch.Storage.Ecto.Migration.down_commands(), &execute/1)
      end
    end
    """

    if File.exists?(migration_file) do
      Mix.shell().info("Migration already exists: #{migration_file}")
    else
      File.write!(migration_file, migration_content)
      Mix.shell().info("Generated migration: #{migration_file}")
    end
  end

  defp priv_dir(repo) do
    repo.config()[:priv] || "priv/#{repo |> Module.split() |> List.last() |> Macro.underscore()}"
  end

  @doc false
  def timestamp do
    {{year, month, day}, {hour, minute, second}} = :calendar.local_time()

    String.pad_leading("#{year}", 4, "0") <>
      String.pad_leading("#{month}", 2, "0") <>
      String.pad_leading("#{day}", 2, "0") <>
      String.pad_leading("#{hour}", 2, "0") <>
      String.pad_leading("#{minute}", 2, "0") <>
      String.pad_leading("#{second}", 2, "0")
  end
end
