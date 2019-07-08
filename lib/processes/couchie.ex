defmodule Processes.Couchie do
  use GenServer

  def init(init_arg) do
    {:ok, init_arg}
  end

  def start_link(opts \\ []) do
    result = GenServer.start_link(__MODULE__, opts)

    admin_user = Application.get_env(:couchie, Couchie)[:admin]

    Application.get_env(:couchie, Couchie)[:buckets]
		|> Enum.each(fn(bucket) -> start(bucket, admin_user) end)

    result
  end

  def start(bucket),
    do: Couchie.open(String.to_atom(bucket), 10, 'localhost:8091', to_charlist(bucket))

  def start(bucket, admin_user),
    do: Couchie.open(String.to_atom(bucket), 10, 'localhost:8091', to_charlist(bucket), admin_user[:user], admin_user[:password])

end
