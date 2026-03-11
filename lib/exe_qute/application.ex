defmodule ExeQute.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    if Code.ensure_loaded?(Kino.SmartCell) do
      Kino.SmartCell.register(ExeQute.ConnectionCell)
      Kino.SmartCell.register(ExeQute.QueryCell)
      Kino.SmartCell.register(ExeQute.SubscribeCell)
      Kino.SmartCell.register(ExeQute.ChartCell)
    end

    children = [
      {Registry, keys: :duplicate, name: ExeQute.Registry},
      {Registry, keys: :unique, name: ExeQute.SubscriberRegistry},
      {DynamicSupervisor, name: ExeQute.SubscriberSupervisor, strategy: :one_for_one}
    ]

    opts = [strategy: :one_for_one, name: ExeQute.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
