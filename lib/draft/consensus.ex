defmodule Draft.Consensus do
  use GenFsm.Behaviour

  @heartbeat_timeout 75
  @election_timeout_minimum 150
  @election_timeout_maximum @election_timeout_minimum * 2
  @election_timeout :crypto.rand_uniform(@election_timeout_minimum, @election_timeout_maximum)

  defrecord State, current_term: 0, voted_for: nil, log: [], election_timer: nil, heartbeat_timer: nil
  defrecord RequestVote, term: nil, candidate_id: nil, last_log_index: nil, last_log_term: nil
  defrecord RequestVoteResult, term: nil, vote_granted: nil
  defrecord AppendEntries, term: nil, leader_id: nil, prev_log_index: nil, prev_log_term: nil, entries: [], commit_index: nil
  defrecord AppendEntriesResult, term: nil, success: nil

  def send_event(node, message) do
    :gen_fsm.send_event(node, message)
  end

  def send_event_after(node, message) do
    :gen_fsm.send_event_after(node, message)
  end

  def start_link(state // State.new) do
    state.update(election_timer: start_timer(@election_timeout))
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
      election_timer = reset_timer(state.election_timer, @election_timeout)
      state = state.update(current_term: current_term, voted_for: request_vote.candidate_id, election_timer: election_timer)
      vote_granted = true
    else
      vote_granted = false
    end

    request_vote_result = RequestVoteResult.new(term: state.current_term, vote_granted: vote_granted)
    send_event(request_vote.candidate_id, request_vote_result)
    next_state(:follower, state)
  end

  # TODO: If existing entries conflict with new entries, delete all existing
  # entries starting with first conflicting entry
  # TODO: Apply newly committed entries to state machine
  def follower(append_entries = AppendEntries[], state) do
    state = state.update(current_term: Enum.max([append_entries.term, state.current_term]))

    if accept_entries?(append_entries, state) do
      state = accept_entries(append_entries, state)
      success = true
    else
      success = false
    end

    append_entries_result = AppendEntriesResult.new(term: state.current_term, success: success)
    send_event(append_entries.leader_id, append_entries_result)
    next_state(:follower, state)
  end

  # TODO: Start new election by sending out request_vote events
  def follower(:election_timeout, state) do
    state = state.update_current_term(&1 + 1)
    state = state.update_election_timer(reset_timer(&1, @election_timeout))
    next_state(:candidate, state)
  end

  def candidate(request_vote = RequestVote[], state) do
    if higher_term?(request_vote.term, state.current_term) do
      follower(request_vote, state)
    else
      request_vote_result = RequestVoteResult.new(term: state.current_term, vote_granted: false)
      send_event(request_vote.candidate_id, request_vote_result)
      next_state(:candidate, state)
    end
  end

  def candidate(append_entries = AppendEntries[], state) do
    if not stale_term?(append_entries.term, state.current_term) do
      follower(append_entries, state)
    else
      append_entries_result = AppendEntriesResult.new(term: state.current_term, success: false)
      send_event(append_entries.leader_id, append_entries_result)
      next_state(:candidate, state)
    end
  end

  # TODO: Start new election by sending out request_vote events
  # TODO: Handle incoming request_vote_result events
  def candidate(:election_timeout, state) do
    state = state.update_current_term(&1 + 1)
    state = state.update_election_timer(reset_timer(&1, @election_timeout))
    next_state(:candidate, state)
  end

  # TODO: Send heartbeat at interval
  def leader(request_vote = RequestVote[], state) do
    if higher_term?(request_vote.term, state.current_term) do
      follower(request_vote, state)
    else
      request_vote_result = RequestVoteResult.new(term: state.current_term, vote_granted: false)
      send_event(request_vote.candidate_id, request_vote_result)
      next_state(:leader, state)
    end
  end

  def leader(append_entries = AppendEntries[], state) do
    if not stale_term?(append_entries.term, state.current_term) do
      follower(append_entries, state)
    else
      append_entries_result = AppendEntriesResult.new(term: state.current_term, success: false)
      send_event(append_entries.leader_id, append_entries_result)
      next_state(:leader, state)
    end
  end

  # TODO: Send empty append_entries event as a heartbeat to all nodes
  def leader(:heartbeat_timeout, state) do
    next_state(:leader, state)
  end

  defp next_state(state_name, state) do
    { :next_state, state_name, state }
  end

  defp stale_term?(term, current_term), do: term < current_term
  defp current_term?(term, current_term), do: term == current_term
  defp higher_term?(term, current_term), do: term > current_term

  defp grant_vote?(request_vote, state) do
    higher_term?(request_vote.term, state.current_term) or
      vote_available?(request_vote, state)
  end

  defp vote_available?(request_vote, state) do
    current_term?(request_vote.term, state.current_term) and
      state.voted_for in [nil, request_vote.candidate_id]
  end

  defp accept_entries?(append_entries, state) do
    not stale_term?(append_entries.term, state.current_term) and
      contains_prev_log?(append_entries, state)
  end

  defp contains_prev_log?(append_entries, state) do
    prev_log_index = append_entries.prev_log_index
    prev_log_term = append_entries.prev_log_term
    finder = match?({ ^prev_log_index, ^prev_log_term, _ }, &1)
    Enum.any?(state.log, finder)
  end

  # TODO: Only append new entries
  defp accept_entries(append_entries, state) do
    log = state.log ++ append_entries.entries
    state.update(log: log)
  end

  if Mix.env != :test do
    defp reset_timer(timer, timeout) do
      if timer, do: cancel_timer(timer)
      start_timer(timeout)
    end

    defp start_timer(timeout) do
      :gen_fsm.start_timer(timeout, :timeout)
    end

    defp cancel_timer(timer) do
      :gen_fsm.cancel_timer(timer)
    end
  else
    defp reset_timer(timer, _) do
      timer
    end

    defp start_timer(_) do
      nil
    end
  end
end
