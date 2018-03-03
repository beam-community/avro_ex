defmodule AvroEx.Mixfile do
  use Mix.Project

  def project do
    [app: :avro_ex,
     version: "0.1.0-beta.3",
     elixir: "~> 1.4",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     aliases: aliases(),
     package: package(),
     description: "A pure-elixir avro encoding/decoding library",
     deps: deps()]
  end

  def application do
    # Specify extra applications you'll use from Erlang/Elixir
    [extra_applications: [:logger, :ecto]]
  end

  defp aliases do
    ["compile": ["compile --warnings-as-errors"]]
  end

  defp deps do
    [{:poison, "~> 3.1.0"},
     {:ex_doc, "~> 0.18.0", only: :dev, runtime: false},
     {:ecto, "~> 2.1.0 or ~> 2.2.0"}]
  end

  defp package do
    [licenses: ["MIT"],
     maintainers: ["cjpoll@gmail.com"],
     links: %{"Github" => "http://github.com/cjpoll/avro_ex"}
    ]
  end
end
