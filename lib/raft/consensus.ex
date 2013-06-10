defmodule Raft.Consensus do
  use GenFsm.Behaviour

  @heartbeat_timeout 75
  @election_timeout_minimum 150
  @election_timeout_maximum @election_timeout_minimum * 2
  @election_timeout :crypto.rand_uniform(@election_timeout_minimum, @election_timeout_maximum)

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
    { :ok, :follower, [], @timeout }
  end

  def follower(:heartbeat, state) do
    { :next_state, :follower, state, @timeout }
  end

  def follower(_, state) do
    { :next_state, :candidate, state, @timeout }
  end

  def candidate(_, state) do
    # start_election
    { :next_state, :candidate, state, @timeout }
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
