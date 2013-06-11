defmodule Raft.Consensus do
  use GenFsm.Behaviour, async: true

  @heartbeat_timeout 75
  @election_timeout_minimum 150
  @election_timeout_maximum @election_timeout_minimum * 2
  @election_timeout :crypto.rand_uniform(@election_timeout_minimum, @election_timeout_maximum)

  defrecord State, current_term: 0, voted_for: nil, log: []

  defrecord RequestVote, term: nil, candidate_id: nil, last_log_index: nil, last_log_term: nil
  defrecord RequestVoteResult, term: nil, vote_granted: nil

  def start_link(state // State.new) do
    :gen_fsm.start_link(__MODULE__, state, [])
  end

  def init(state) do
    { :ok, :follower, state }
  end

  def follower(RequestVote[term: term], state = State[current_term: current_term])
      when term < current_term do
    # reply RequestVoteResult.new(term: current_term, vote_granted: false)
    { :next_state, :follower, state }
  end

  def follower(request_vote = RequestVote[], state = State[]) do
    if request_vote.term > state.current_term do
      state = state.update(current_term: request_vote.term, voted_for: nil)
    end

    # TODO: Also compare the candidate's log against your own
    if state.voted_for == nil or state.voted_for == request_vote.candidate_id do
      state = state.update(voted_for: request_vote.candidate_id)
      # TODO: Reset election timeout
    end

    # reply RequestVoteResult.new(term: state.current_term, vote_granted: true)
    { :next_state, :follower, state }
  end

  def candidate(request_vote = RequestVote[], state = State[]) do
    if request_vote.term > state.current_term do
      follower(request_vote, state)
    else
      # reply RequestVoteResult.new(term: state.current_term, vote_granted: false)
      { :next_state, :candidate, state }
    end
  end

  def leader(request_vote = RequestVote[], state = State[]) do
    if request_vote.term > state.current_term do
      follower(request_vote, state)
    else
      # reply RequestVoteResult.new(term: state.current_term, vote_granted: false)
      { :next_state, :leader, state }
    end
  end
end
