defmodule Draft.Consensus do
  use GenFsm.Behaviour

  @heartbeat_timeout 75
  @election_timeout_minimum 150
  @election_timeout_maximum @election_timeout_minimum * 2
  @election_timeout :crypto.rand_uniform(@election_timeout_minimum, @election_timeout_maximum)

  defrecord State, current_term: 0, voted_for: nil, log: []
  defrecord RequestVote, term: nil, candidate_id: nil, last_log_index: nil, last_log_term: nil
  defrecord RequestVoteResult, term: nil, vote_granted: nil

  def send_event(node, message) do
    :gen_fsm.send_event(node, message)
  end

  def start_link(state // State.new) do
    :gen_fsm.start_link(__MODULE__, state, [])
  end

  def init(state) do
    { :ok, :follower, state }
  end

  def follower(RequestVote[term: term, candidate_id: candidate_id], state = State[current_term: current_term, voted_for: voted_for])
      when term < current_term
      when term == current_term and voted_for != nil and voted_for != candidate_id do
    send_event(candidate_id, RequestVoteResult.new(term: current_term, vote_granted: false))
    next_state(:follower, state)
  end

  # TODO: Compare the candidate's log against your own
  # TODO: Reset election timeout
  def follower(RequestVote[term: term, candidate_id: candidate_id], state = State[current_term: current_term]) do
    state = state.update(current_term: Enum.max([term, current_term]), voted_for: candidate_id)
    send_event(candidate_id, RequestVoteResult.new(term: state.current_term, vote_granted: true))
    next_state(:follower, state)
  end

  def candidate(request_vote = RequestVote[term: term], state = State[current_term: current_term])
      when term > current_term do
    follower(request_vote, state)
  end

  def candidate(RequestVote[candidate_id: candidate_id], state = State[current_term: current_term]) do
    send_event(candidate_id, RequestVoteResult.new(term: current_term, vote_granted: false))
    next_state(:candidate, state)
  end

  def leader(request_vote = RequestVote[term: term], state = State[current_term: current_term])
      when term > current_term do
    follower(request_vote, state)
  end

  def leader(RequestVote[candidate_id: candidate_id], state = State[current_term: current_term]) do
    send_event(candidate_id, RequestVoteResult.new(term: current_term, vote_granted: false))
    next_state(:leader, state)
  end

  defp next_state(state_name, state) do
    { :next_state, state_name, state }
  end
end
