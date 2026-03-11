defmodule ExeQute.DateTime do
  @moduledoc false

  @epoch_unix_nanos 946_684_800_000_000_000

  def to_kdb(%DateTime{} = dt) do
    DateTime.to_unix(dt, :nanosecond) - @epoch_unix_nanos
  end

  def from_kdb(nanos) do
    DateTime.from_unix!(nanos + @epoch_unix_nanos, :nanosecond)
  end
end
