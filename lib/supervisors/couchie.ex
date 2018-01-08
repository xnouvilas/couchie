defmodule Supervisors.Couchie do
  use Supervisor

  def start_link(_opts \\ []) do
    Supervisor.start_link(__MODULE__, :ok)
  end

  ## Callbacks

  def init(_opts) do
    children = [
      worker(Processes.Couchie, []),
    ]

    opts = [strategy: :one_for_one]

    supervise(children, opts)
  end

end
