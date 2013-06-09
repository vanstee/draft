defmodule Raft.Consensus do
  use GenFsm.Behaviour

  def heartbeat(node) do
    :gen_fsm.send_event(node, :heartbeat)
  end

  def vote(node) do
    :gen_fsm.send_event(node, { :vote, node })
  end

  def start_link() do
    :gen_fsm.start_link(__MODULE__, [], [])
  end

  def init([]) do
    { :ok, :follower, [], 500 }
  end

  def follower(:heartbeat, state) do
    { :next_state, :follower, state, 500 }
  end

  def follower(_, state) do
    { :next_state, :candidate, state, 500 }
  end

  def candidate(_, state) do
    # start_election
    { :next_state, :candidate, state, 500 }
  end

  def candidate({ :vote, node }, state) do
    # record vote

    if true # elected
      { :next_state, :leader, state }
    else
      { :next_state, :candidate, state }
    end
  end
end
