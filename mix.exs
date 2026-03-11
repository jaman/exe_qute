defmodule ExeQute.MixProject do
  use Mix.Project

  def project do
    [
      app: :exe_qute,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      description: description(),
      package: package(),
      docs: docs(),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {ExeQute.Application, []}
    ]
  end

  defp description do
    "An Elixir client for KDB+: querying, parameterized queries, async publish, and pub/sub."
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/jaman/exe_qute"}
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: [
        "README.md",
        "guides/quick_start.md",
        "guides/livebook_smart_cells.md"
      ],
      groups_for_extras: [
        Guides: ~r/guides\//
      ],
      groups_for_modules: [
        "Livebook Integration": [
          ExeQute.Explorer,
          ExeQute.EChart
        ],
        "Pub/Sub": [
          ExeQute.Subscriber
        ]
      ]
    ]
  end

  defp deps do
    [
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:kino, "~> 0.14", optional: true},
      {:vega_lite, "~> 0.1", optional: true},
      {:kino_vega_lite, "~> 0.1", optional: true}
    ]
  end
end
