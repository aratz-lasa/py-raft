async def append_entries(term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
    """
    Invoked by leader to replicate log entries; also used as heartbeat
    :param term:            leader’s term
    :param leader_id:       so follower can redirect clients
    :param prev_log_index:  index of log entry immediately precedingnew ones
    :param prev_log_term:   term of prevLogIndex entry
    :param entries:         log entries to store (empty for heartbeat; may send more than one for efficiency)
    :param leader_commit:   leader’s commitIndex
    :return:
            term:       currentTerm, for leader to update itself
            success:    true if follower contained entry matching prevLogIndex and prevLogTerm
    """
    pass


async def request_vote(term, candidate_id, last_log_index, last_log_term):
    """
    Invoked by candidates to gather votes
    :param term:            candidate’s term
    :param candidate_id:    candidate requesting vote
    :param last_log_index:  index of candidate’s last log entry
    :param last_log_term:   term of candidate’s last log entry
    :return:
            term:           current_term, for candidate to update itself
            vote_granted:   true means candidate received vote
    """
    pass
