defmodule Raft.ConsensusTest do
  use ExUnit.Case

  import Raft.Consensus

  Record.import Raft.Consensus.State, as: :state
  Record.import Raft.Consensus.RequestVote, as: :request_vote

  test 'follower receiving "request_vote" with stale term' do
    candidate_id = make_ref
    result = follower(request_vote(term: 1, candidate_id: candidate_id), state(current_term: 2))
    assert result == { :next_state, :follower, state(current_term: 2) }
  end

  test 'follower receiving "request_vote" with current term when a vote has already been cast' do
    voted_for = make_ref
    candidate_id = make_ref
    result = follower(request_vote(term: 1, candidate_id: candidate_id), state(current_term: 1, voted_for: voted_for))
    assert result == { :next_state, :follower, state(voted_for: voted_for, current_term: 1) }
  end

  test 'follower receiving "request_vote" with current term when no vote has been cast' do
    candidate_id = make_ref
    result = follower(request_vote(term: 1, candidate_id: candidate_id), state(current_term: 1))
    assert result == { :next_state, :follower, state(current_term: 1, voted_for: candidate_id) }
  end

  test 'follower receiving "request_vote" with higher term' do
    voted_for = make_ref
    candidate_id = make_ref
    result = follower(request_vote(term: 2, candidate_id: candidate_id), state(current_term: 1, voted_for: voted_for))
    assert result == { :next_state, :follower, state(voted_for: candidate_id, current_term: 2) }
  end

  test 'candidate receiving "request_vote" with stale term' do
    candidate_id = make_ref
    result = candidate(request_vote(term: 1, candidate_id: candidate_id), state(current_term: 2))
    assert result == { :next_state, :candidate, state(current_term: 2) }
  end

  test 'candidate receiving "request_vote" with current term' do
    candidate_id = make_ref
    result = candidate(request_vote(term: 1, candidate_id: candidate_id), state(current_term: 1))
    assert result == { :next_state, :candidate, state(current_term: 1) }
  end

  test 'candidate receiving "request_vote" with higher term' do
    candidate_id = make_ref
    result = candidate(request_vote(term: 2, candidate_id: candidate_id), state(current_term: 1))
    assert result == { :next_state, :follower, state(current_term: 2, voted_for: candidate_id) }
  end

  test 'leader receiving "request_vote" with stale term' do
    candidate_id = make_ref
    result = leader(request_vote(term: 1, candidate_id: candidate_id), state(current_term: 2))
    assert result == { :next_state, :leader, state(current_term: 2) }
  end

  test 'leader receiving "request_vote" with current term' do
    candidate_id = make_ref
    result = leader(request_vote(term: 1, candidate_id: candidate_id), state(current_term: 1))
    assert result == { :next_state, :leader, state(current_term: 1) }
  end

  test 'leader receiving "request_vote" with higher term' do
    candidate_id = make_ref
    result = leader(request_vote(term: 2, candidate_id: candidate_id), state(current_term: 1))
    assert result == { :next_state, :follower, state(current_term: 2, voted_for: candidate_id) }
  end
end
