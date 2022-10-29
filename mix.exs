defmodule LibclusterTailscale.MixProject do
  use Mix.Project

  def project do
    [
      app: :libcluster_tailscale,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:libcluster, "~> 3.0"},
      {:jason, "~> 1.4"}
    ]
  end
end
