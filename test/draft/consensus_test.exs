defmodule Draft.ConsensusTest do
  use ExUnit.Case, async: true

  import TestHelper

  alias Draft.Consensus

  test 'sending an event to another node' do
    Consensus.send_event(self, :event)
    assert_receive_event(:event)
  end

  test 'follower receiving "request_vote" with stale term' do
    result = Consensus.follower(Consensus.RequestVote.new(term: 1, candidate_id: self), Consensus.State.new(current_term: 2))
    assert result == { :next_state, :follower, Consensus.State.new(current_term: 2) }
    assert_receive_event(Consensus.RequestVoteResult.new(term: 2, vote_granted: false))
  end

  test 'follower receiving "request_vote" with current term when a vote has already been cast' do
    voted_for = make_ref
    result = Consensus.follower(Consensus.RequestVote.new(term: 1, candidate_id: self), Consensus.State.new(current_term: 1, voted_for: voted_for))
    assert result == { :next_state, :follower, Consensus.State.new(voted_for: voted_for, current_term: 1) }
    assert_receive_event(Consensus.RequestVoteResult.new(term: 1, vote_granted: false))
  end

  test 'follower receiving "request_vote" with current term when no vote has been cast' do
    result = Consensus.follower(Consensus.RequestVote.new(term: 1, candidate_id: self), Consensus.State.new(current_term: 1))
    assert result == { :next_state, :follower, Consensus.State.new(current_term: 1, voted_for: self) }
    assert_receive_event(Consensus.RequestVoteResult.new(term: 1, vote_granted: true))
  end

  test 'follower receiving "request_vote" with higher term' do
    voted_for = make_ref
    result = Consensus.follower(Consensus.RequestVote.new(term: 2, candidate_id: self), Consensus.State.new(current_term: 1, voted_for: voted_for))
    assert result == { :next_state, :follower, Consensus.State.new(voted_for: self, current_term: 2) }
    assert_receive_event(Consensus.RequestVoteResult.new(term: 2, vote_granted: true))
  end

  test 'follower receiving "append_entries" with stale term' do
    result = Consensus.follower(Consensus.AppendEntries.new(term: 1, leader_id: self), Consensus.State.new(current_term: 2, log: []))
    assert result == { :next_state, :follower, Consensus.State.new(current_term: 2, log: []) }
    assert_receive_event(Consensus.AppendEntriesResult.new(term: 2, success: false))
  end

  test 'follower receiving "append_entries" with the current or higher term and a log that does not contain the previous entry' do
    result = Consensus.follower(Consensus.AppendEntries.new(term: 1, leader_id: self, prev_log_index: 1, prev_log_term: 2), Consensus.State.new(current_term: 1, log: []))
    assert result == { :next_state, :follower, Consensus.State.new(current_term: 1, log: []) }
    assert_receive_event(Consensus.AppendEntriesResult.new(term: 1, success: false))
  end

  test 'follower receiving "append_entries" with the current or higher term and a log that contains the previous entry' do
    result = Consensus.follower(Consensus.AppendEntries.new(term: 1, leader_id: self, prev_log_index: 1, prev_log_term: 2, entries: [{ 2, 3, 'appended entry' }]), Consensus.State.new(current_term: 1, log: [{ 1, 2, 'previous entry' }]))
    assert result == { :next_state, :follower, Consensus.State.new(current_term: 1, log: [{ 1, 2, 'previous entry' }, { 2, 3, 'appended entry' }]) }
    assert_receive_event(Consensus.AppendEntriesResult.new(term: 1, success: true))
  end

  test 'follower receiving "append_entries" with higher term' do
    result = Consensus.follower(Consensus.AppendEntries.new(term: 2, leader_id: self, prev_log_index: 1, prev_log_term: 2, entries: [{ 2, 3, 'appended entry' }]), Consensus.State.new(current_term: 1, log: [{ 1, 2, 'previous entry' }]))
    assert result == { :next_state, :follower, Consensus.State.new(current_term: 2, log: [{ 1, 2, 'previous entry' }, { 2, 3, 'appended entry' }]) }
    assert_receive_event(Consensus.AppendEntriesResult.new(term: 2, success: true))
  end

  test 'candidate receiving "request_vote" with stale term' do
    result = Consensus.candidate(Consensus.RequestVote.new(term: 1, candidate_id: self), Consensus.State.new(current_term: 2))
    assert result == { :next_state, :candidate, Consensus.State.new(current_term: 2) }
    assert_receive_event(Consensus.RequestVoteResult.new(term: 2, vote_granted: false))
  end

  test 'candidate receiving "request_vote" with current term' do
    result = Consensus.candidate(Consensus.RequestVote.new(term: 1, candidate_id: self), Consensus.State.new(current_term: 1))
    assert result == { :next_state, :candidate, Consensus.State.new(current_term: 1) }
    assert_receive_event(Consensus.RequestVoteResult.new(term: 1, vote_granted: false))
  end

  test 'candidate receiving "request_vote" with higher term' do
    result = Consensus.candidate(Consensus.RequestVote.new(term: 2, candidate_id: self), Consensus.State.new(current_term: 1))
    assert result == { :next_state, :follower, Consensus.State.new(current_term: 2, voted_for: self) }
    assert_receive_event(Consensus.RequestVoteResult.new(term: 2, vote_granted: true))
  end

  test 'leader receiving "request_vote" with stale term' do
    result = Consensus.leader(Consensus.RequestVote.new(term: 1, candidate_id: self), Consensus.State.new(current_term: 2))
    assert result == { :next_state, :leader, Consensus.State.new(current_term: 2) }
    assert_receive_event(Consensus.RequestVoteResult.new(term: 2, vote_granted: false))
  end

  test 'leader receiving "request_vote" with current term' do
    result = Consensus.leader(Consensus.RequestVote.new(term: 1, candidate_id: self), Consensus.State.new(current_term: 1))
    assert result == { :next_state, :leader, Consensus.State.new(current_term: 1) }
    assert_receive_event(Consensus.RequestVoteResult.new(term: 1, vote_granted: false))
  end

  test 'leader receiving "request_vote" with higher term' do
    result = Consensus.leader(Consensus.RequestVote.new(term: 2, candidate_id: self), Consensus.State.new(current_term: 1))
    assert result == { :next_state, :follower, Consensus.State.new(current_term: 2, voted_for: self) }
    assert_receive_event(Consensus.RequestVoteResult.new(term: 2, vote_granted: true))
  end
end
