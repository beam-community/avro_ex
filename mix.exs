defmodule AvroEx.Mixfile do
  use Mix.Project

  def project do
    [
      app: :avro_ex,
      version: "1.0.0",
      elixir: "~> 1.6",
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      package: package(),
      elixirc_paths: elixirc_paths(Mix.env()),
      description: "A pure-elixir avro encoding/decoding library",
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:ecto, :logger]]
  end

  defp aliases do
    [compile: ["compile --warnings-as-errors"]]
  end

  defp deps do
    [
      {:ecto, "~> 3.0"},
      {:jason, "~> 1.1"},
      {:credo, "~> 1.0", only: :dev, runtime: false},
      {:dialyxir, "~> 1.0.0-rc.6", only: :dev, runtime: false},
      {:ex_doc, "~> 0.20", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      maintainers: ["doomspork", "cjpoll"],
      links: %{"Github" => "http://github.com/beam-community/avro_ex"}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
