defmodule AvroEx.Mixfile do
  use Mix.Project

  @url "http://github.com/beam-community/avro_ex"
  @version "1.2.0"

  def project do
    [
      app: :avro_ex,
      version: @version,
      elixir: "~> 1.6",
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      package: package(),
      elixirc_paths: elixirc_paths(Mix.env()),
      name: "AvroEx",
      description: "An Avro encoding/decoding library written in pure Elixir",
      docs: docs(),
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp aliases do
    []
  end

  defp deps do
    [
      {:ecto, "~> 3.0"},
      {:jason, "~> 1.1"},
      {:typed_struct, "~> 0.3.0"},
      {:credo, "~> 1.0", only: :dev, runtime: false},
      {:dialyxir, "~> 1.1", only: :dev, runtime: false},
      {:ex_doc, "~> 0.20", only: :dev, runtime: false}
    ]
  end

  defp docs do
    [
      main: "AvroEx",
      source_url: @url,
      source_ref: "v#{@version}",
      groups_for_modules: [
        Schema: ~r/Schema/,
        Ecto: [AvroEx.Term]
      ],
      extras: []
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      maintainers: ["doomspork", "cjpoll", "davydog187"],
      links: %{"Github" => @url}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
