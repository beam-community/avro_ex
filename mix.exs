defmodule AvroEx.Mixfile do
  use Mix.Project

  def project do
    [app: :avro_ex,
     version: "0.1.0",
     elixir: "~> 1.4",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     aliases: aliases(),
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
     {:ecto, "~> 2.1.0"}]
  end
end
