defmodule Draft.Timer do
  def reset(timer, timeout, node) do
    if timer, do: cancel(timer)
    start(timeout, node)
  end

  def start(timeout, node) do
   { :ok, { _timestamp, timer } } = :timer.apply_after(timeout, Consensus, :send_event, [node, :timeout])
   timer
  end

  def cancel(timer) do
    :timer.cancel(timer)
  end
end
