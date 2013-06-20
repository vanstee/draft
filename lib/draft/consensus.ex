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

  # TODO: Compare the candidate's log against your own
  # TODO: Reset election timeout
  def follower(request_vote = RequestVote[], state) do
    if grant_vote?(request_vote, state) do
      current_term = Enum.max([request_vote.term, state.current_term])
      state = state.update(current_term: current_term, voted_for: request_vote.candidate_id)
      vote_granted = true
    else
      vote_granted = false
    end

    request_vote_result = RequestVoteResult.new(term: state.current_term, vote_granted: vote_granted)
    send_event(request_vote.candidate_id, request_vote_result)
    next_state(:follower, state)
  end

  def candidate(request_vote = RequestVote[], state) do
    if higher_term?(request_vote, state) do
      follower(request_vote, state)
    else
      request_vote_result = RequestVoteResult.new(term: state.current_term, vote_granted: false)
      send_event(request_vote.candidate_id, request_vote_result)
      next_state(:candidate, state)
    end
  end

  def leader(request_vote = RequestVote[], state) do
    if higher_term?(request_vote, state) do
      follower(request_vote, state)
    else
      request_vote_result = RequestVoteResult.new(term: state.current_term, vote_granted: false)
      send_event(request_vote.candidate_id, request_vote_result)
      next_state(:leader, state)
    end
  end

  defp next_state(state_name, state) do
    { :next_state, state_name, state }
  end

  defp stale_term?(request_vote, state), do: request_vote.term < state.current_term
  defp current_term?(request_vote, state), do: request_vote.term == state.current_term
  defp higher_term?(request_vote, state), do: request_vote.term > state.current_term

  defp grant_vote?(request_vote, state) do
    higher_term?(request_vote, state) or
      vote_available?(request_vote, state)
  end

  defp vote_available?(request_vote, state) do
    current_term?(request_vote, state) and
      (state.voted_for == nil or state.voted_for == request_vote.candidate_id)
  end
end
