defmodule Draft.ConsensusTest do
  use ExUnit.Case, async: true

  import TestHelper

  alias Draft.Consensus
  alias Draft.NullTimer

  test 'sending an event to another node' do
    Consensus.send_event(self, :event)

    assert_receive_event(:event)
  end

  test 'follower receiving "request_vote" with stale term' do
    request_vote = Consensus.RequestVote.new(term: 1, candidate_id: self)
    initial_state = Consensus.State.new(current_term: 2, timer_module: NullTimer)
    transition = Consensus.follower(request_vote, initial_state)

    final_state = Consensus.State.new(current_term: 2, timer_module: NullTimer)
    request_vote_result = Consensus.RequestVoteResult.new(term: 2, vote_granted: false)

    assert transition == { :next_state, :follower, final_state }
    assert_receive_event(request_vote_result)
 end

  test 'follower receiving "request_vote" with current term when a vote has already been cast' do
    voted_for = make_ref
    request_vote = Consensus.RequestVote.new(term: 1, candidate_id: self)
    initial_state = Consensus.State.new(current_term: 1, voted_for: voted_for, timer_module: NullTimer)
    transition = Consensus.follower(request_vote, initial_state)

    final_state = Consensus.State.new(voted_for: voted_for, current_term: 1, timer_module: NullTimer)
    request_vote_result = Consensus.RequestVoteResult.new(term: 1, vote_granted: false)

    assert transition == { :next_state, :follower, final_state }
    assert_receive_event(request_vote_result)
  end

  test 'follower receiving "request_vote" with current term when no vote has been cast' do
    request_vote = Consensus.RequestVote.new(term: 1, candidate_id: self)
    initial_state = Consensus.State.new(current_term: 1, timer_module: NullTimer)
    transition = Consensus.follower(request_vote, initial_state)

    final_state = Consensus.State.new(current_term: 1, voted_for: self, timer_module: NullTimer)
    request_vote_result = Consensus.RequestVoteResult.new(term: 1, vote_granted: true)

    assert transition == { :next_state, :follower, final_state }
    assert_receive_event(request_vote_result)
  end

  test 'follower receiving "request_vote" with higher term' do
    voted_for = make_ref
    request_vote = Consensus.RequestVote.new(term: 2, candidate_id: self)
    initial_state = Consensus.State.new(current_term: 1, voted_for: voted_for, timer_module: NullTimer)
    transition = Consensus.follower(request_vote, initial_state)

    final_state = Consensus.State.new(voted_for: self, current_term: 2, timer_module: NullTimer)
    request_vote_result = Consensus.RequestVoteResult.new(term: 2, vote_granted: true)

    assert transition == { :next_state, :follower, final_state }
    assert_receive_event(request_vote_result)
  end

  test 'follower receiving "append_entries" with stale term' do
    append_entries = Consensus.AppendEntries.new(term: 1, leader_id: self)
    initial_state = Consensus.State.new(current_term: 2, log: [], timer_module: NullTimer)
    transition = Consensus.follower(append_entries, initial_state)

    final_state = Consensus.State.new(current_term: 2, log: [], timer_module: NullTimer)
    append_entries_result = Consensus.AppendEntriesResult.new(term: 2, success: false)

    assert transition == { :next_state, :follower, final_state }
    assert_receive_event(append_entries_result)
  end

  test 'follower receiving "append_entries" with the current or higher term and a log that does not contain the previous entry' do
    append_entries = Consensus.AppendEntries.new(term: 1, leader_id: self, prev_log_index: 1, prev_log_term: 2)
    initial_state = Consensus.State.new(current_term: 1, log: [], timer_module: NullTimer)
    transition = Consensus.follower(append_entries, initial_state)

    final_state = Consensus.State.new(current_term: 1, log: [], timer_module: NullTimer)
    append_entries_result = Consensus.AppendEntriesResult.new(term: 1, success: false)

    assert transition == { :next_state, :follower, final_state }
    assert_receive_event(append_entries_result)
  end

  test 'follower receiving "append_entries" with the current or higher term and a log that contains the previous entry' do
    append_entries = Consensus.AppendEntries.new(term: 1, leader_id: self, prev_log_index: 1, prev_log_term: 2, entries: [{ 2, 3, 'appended entry' }])
    initial_state = Consensus.State.new(current_term: 1, log: [{ 1, 2, 'previous entry' }], timer_module: NullTimer)
    transition = Consensus.follower(append_entries, initial_state)

    final_state = Consensus.State.new(current_term: 1, log: [{ 1, 2, 'previous entry' }, { 2, 3, 'appended entry' }], timer_module: NullTimer)
    append_entries_result = Consensus.AppendEntriesResult.new(term: 1, success: true)

    assert transition == { :next_state, :follower, final_state }
    assert_receive_event(append_entries_result)
  end

  test 'follower receiving "append_entries" with higher term' do
    append_entries = Consensus.AppendEntries.new(term: 2, leader_id: self, prev_log_index: 1, prev_log_term: 2, entries: [{ 2, 3, 'appended entry' }])
    initial_state = Consensus.State.new(current_term: 1, log: [{ 1, 2, 'previous entry' }], timer_module: NullTimer)
    transition = Consensus.follower(append_entries, initial_state)

    final_state = Consensus.State.new(current_term: 2, log: [{ 1, 2, 'previous entry' }, { 2, 3, 'appended entry' }], timer_module: NullTimer)
    append_entries_result = Consensus.AppendEntriesResult.new(term: 2, success: true)

    assert transition == { :next_state, :follower, final_state }
    assert_receive_event(append_entries_result)
  end

  test 'follower receiving "election_timeout"' do
    initial_state = Consensus.State.new(current_term: 1, timer_module: NullTimer)
    transition = Consensus.follower(:election_timeout, initial_state)

    final_state = Consensus.State.new(current_term: 2, timer_module: NullTimer)

    assert transition == { :next_state, :candidate, final_state }
  end

  test 'candidate receiving "request_vote" with stale term' do
    request_vote = Consensus.RequestVote.new(term: 1, candidate_id: self)
    initial_state = Consensus.State.new(current_term: 2, timer_module: NullTimer)
    transition = Consensus.candidate(request_vote, initial_state)

    final_state = Consensus.State.new(current_term: 2, timer_module: NullTimer)
    request_vote_result = Consensus.RequestVoteResult.new(term: 2, vote_granted: false)

    assert transition == { :next_state, :candidate, final_state }
    assert_receive_event(request_vote_result)
  end

  test 'candidate receiving "request_vote" with current term' do
    request_vote = Consensus.RequestVote.new(term: 1, candidate_id: self)
    initial_state = Consensus.State.new(current_term: 1, timer_module: NullTimer)
    transition = Consensus.candidate(request_vote, initial_state)

    final_state = Consensus.State.new(current_term: 1, timer_module: NullTimer)
    request_vote_result = Consensus.RequestVoteResult.new(term: 1, vote_granted: false)

    assert transition == { :next_state, :candidate, final_state }
    assert_receive_event(request_vote_result)
  end

  test 'candidate receiving "request_vote" with higher term' do
    request_vote = Consensus.RequestVote.new(term: 2, candidate_id: self)
    initial_state = Consensus.State.new(current_term: 1, timer_module: NullTimer)
    transition = Consensus.candidate(request_vote, initial_state)

    final_state = Consensus.State.new(current_term: 2, voted_for: self, timer_module: NullTimer)
    request_vote_result = Consensus.RequestVoteResult.new(term: 2, vote_granted: true)

    assert transition == { :next_state, :follower, final_state }
    assert_receive_event(request_vote_result)
  end

  test 'candidate receiving "append_entries" with stale term' do
    append_entries = Consensus.AppendEntries.new(term: 1, leader_id: self)
    initial_state = Consensus.State.new(current_term: 2, timer_module: NullTimer)
    transition = Consensus.candidate(append_entries, initial_state)

    final_state = Consensus.State.new(current_term: 2, timer_module: NullTimer)
    append_entries_result = Consensus.AppendEntriesResult.new(term: 2, success: false)

    assert transition == { :next_state, :candidate, final_state }
    assert_receive_event(append_entries_result)
  end

  test 'candidate receiving "append_entries" with current term' do
    append_entries = Consensus.AppendEntries.new(term: 1, leader_id: self)
    initial_state = Consensus.State.new(current_term: 1, timer_module: NullTimer)
    transition = Consensus.candidate(append_entries, initial_state)

    final_state = Consensus.State.new(current_term: 1, timer_module: NullTimer)
    append_entries_result = Consensus.AppendEntriesResult.new(term: 1, success: false)

    assert transition == { :next_state, :follower, final_state }
    assert_receive_event(append_entries_result)
  end

  test 'candidate receiving "append_entries" with higher term' do
    append_entries = Consensus.AppendEntries.new(term: 2, leader_id: self)
    initial_state = Consensus.State.new(current_term: 1, timer_module: NullTimer)
    transition = Consensus.candidate(append_entries, initial_state)

    final_state = Consensus.State.new(current_term: 2, timer_module: NullTimer)
    append_entries_result = Consensus.AppendEntriesResult.new(term: 2, success: false)

    assert transition == { :next_state, :follower, final_state }
    assert_receive_event(append_entries_result)
  end

  test 'candidate receiving "election_timeout"' do
    initial_state = Consensus.State.new(current_term: 1, timer_module: NullTimer)
    transition = Consensus.candidate(:election_timeout, initial_state)

    final_state = Consensus.State.new(current_term: 2, timer_module: NullTimer)

    assert transition == { :next_state, :candidate, final_state }
  end

  test 'leader receiving "request_vote" with stale term' do
    request_vote = Consensus.RequestVote.new(term: 1, candidate_id: self)
    initial_state = Consensus.State.new(current_term: 2, timer_module: NullTimer)
    transition = Consensus.leader(request_vote, initial_state)

    final_state = Consensus.State.new(current_term: 2, timer_module: NullTimer)
    request_vote_result = Consensus.RequestVoteResult.new(term: 2, vote_granted: false)

    assert transition == { :next_state, :leader, final_state }
    assert_receive_event(request_vote_result)
  end

  test 'leader receiving "request_vote" with current term' do
    request_vote = Consensus.RequestVote.new(term: 1, candidate_id: self)
    initial_state = Consensus.State.new(current_term: 1, timer_module: NullTimer)
    transition = Consensus.leader(request_vote, initial_state)

    final_state = Consensus.State.new(current_term: 1, timer_module: NullTimer)
    request_vote_result = Consensus.RequestVoteResult.new(term: 1, vote_granted: false)

    assert transition == { :next_state, :leader, final_state }
    assert_receive_event(request_vote_result)
  end

  test 'leader receiving "request_vote" with higher term' do
    request_vote = Consensus.RequestVote.new(term: 2, candidate_id: self)
    initial_state = Consensus.State.new(current_term: 1, timer_module: NullTimer)
    transition = Consensus.leader(request_vote, initial_state)

    final_state = Consensus.State.new(current_term: 2, voted_for: self, timer_module: NullTimer)
    request_vote_result = Consensus.RequestVoteResult.new(term: 2, vote_granted: true)

    assert transition == { :next_state, :follower, final_state }
    assert_receive_event(request_vote_result)
  end

  test 'leader receiving "append_entries" with stale term' do
    append_entries = Consensus.AppendEntries.new(term: 1, leader_id: self)
    initial_state = Consensus.State.new(current_term: 2, timer_module: NullTimer)
    transition = Consensus.leader(append_entries, initial_state)

    final_state = Consensus.State.new(current_term: 2, timer_module: NullTimer)
    append_entries_result = Consensus.AppendEntriesResult.new(term: 2, success: false)

    assert transition == { :next_state, :leader, final_state }
    assert_receive_event(append_entries_result)
  end

  test 'leader receiving "append_entries" with current term' do
    append_entries = Consensus.AppendEntries.new(term: 1, leader_id: self)
    initial_state = Consensus.State.new(current_term: 1, timer_module: NullTimer)
    transition = Consensus.leader(append_entries, initial_state)

    final_state = Consensus.State.new(current_term: 1, timer_module: NullTimer)
    append_entries_result = Consensus.AppendEntriesResult.new(term: 1, success: false)

    assert transition == { :next_state, :follower, final_state }
    assert_receive_event(append_entries_result)
  end

  test 'leader receiving "append_entries" with higher term' do
    append_entries = Consensus.AppendEntries.new(term: 2, leader_id: self)
    initial_state = Consensus.State.new(current_term: 1, timer_module: NullTimer)
    transition = Consensus.leader(append_entries, initial_state)

    final_state = Consensus.State.new(current_term: 2, timer_module: NullTimer)
    append_entries_result = Consensus.AppendEntriesResult.new(term: 2, success: false)

    assert transition == { :next_state, :follower, final_state }
    assert_receive_event(append_entries_result)
  end

  test 'leader receiving "heartbeat_timeout"' do
    initial_state = Consensus.State.new(current_term: 1, timer_module: NullTimer)
    transition = Consensus.leader(:heartbeat_timeout, initial_state)

    final_state = Consensus.State.new(current_term: 1, timer_module: NullTimer)

    assert transition == { :next_state, :leader, final_state }
  end
end
