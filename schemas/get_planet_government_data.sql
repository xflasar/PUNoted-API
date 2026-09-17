-- ============================================================================
-- Useful Queries for Planet Government Terms, Candidates, Motions, & Components
-- ============================================================================

-- 1. Get All Planet Government Terms with Candidate & Winner Details
SELECT 
    t.termid,
    t.admincenterid,
    t.planet_natural_id,
    t.term_start,
    t.term_end,
    t.election_start,
    t.election_end,
    t.parliament_size,
    t.election_ongoing,
    COALESCE(
        json_agg(
            json_build_object(
                'candidate_id', c.id,
                'userid', c.userid,
                'username', c.username,
                'corporation_name', c.corporation_name,
                'corporation_code', c.corporation_code,
                'country_code', c.country_code,
                'votes', c.votes,
                'votes_percentage', c.votes_percentage,
                'is_winner', c.is_winner,
                'start_of_run', c.start_of_run
            ) ORDER BY c.votes DESC
        ) FILTER (WHERE c.id IS NOT NULL), '[]'::json
    ) AS candidates
FROM planet_government_terms t
LEFT JOIN planet_government_candidates c ON c.termid = t.termid
GROUP BY t.termid, t.admincenterid, t.planet_natural_id, t.term_start, t.term_end, 
         t.election_start, t.election_end, t.parliament_size, t.election_ongoing
ORDER BY t.term_start DESC;


-- 2. Get All Planet Motions with Creator Info & Voting Timestamps
SELECT 
    m.motionid,
    m.admincenterid,
    m.naturalid,
    m.name AS motion_name,
    m.status,
    m.creator_id,
    m.creator_username,
    m.created_at,
    m.voting_start,
    m.voting_end
FROM planet_motions m
ORDER BY m.created_at DESC;


-- 3. Get Full Motion Details (Including Votes & Components / Financial & Workforce Details)
SELECT 
    m.motionid,
    m.admincenterid,
    m.naturalid,
    m.name AS motion_name,
    m.status,
    m.creator_username,
    m.created_at,
    COALESCE(
        json_agg(DISTINCT jsonb_build_object(
            'vote_id', v.id,
            'voter_id', v.voter_id,
            'voter_username', v.voter_username,
            'role', v.role,
            'status', v.status,
            'voted_at', v.voted_at
        )) FILTER (WHERE v.id IS NOT NULL), '[]'::json
    ) AS votes,
    COALESCE(
        json_agg(DISTINCT jsonb_build_object(
            'component_id', comp.componentid,
            'type', comp.type,
            'contributor_username', comp.contributor_username,
            'recipient_id', comp.recipient_id,
            'amount', comp.amount,
            'currency', comp.currency,
            'program', comp.program,
            'category', comp.category
        )) FILTER (WHERE comp.componentid IS NOT NULL), '[]'::json
    ) AS components
FROM planet_motions m
LEFT JOIN planet_motion_votes v ON v.motionid = m.motionid
LEFT JOIN planet_motion_components comp ON comp.motionid = m.motionid
GROUP BY m.motionid, m.admincenterid, m.naturalid, m.name, m.status, m.creator_username, m.created_at
ORDER BY m.created_at DESC;


-- 4. Get Contracts Linked to Planet Government Motions (Deduplicated with Admin Center & Planet Info)
SELECT DISTINCT ON (c.id)
    c.id AS contractid,
    COALESCE(m.admincenterid, c.partnerid) AS admincenterid,
    p.name AS planet_name,
    p.naturalid AS planet_natural_id,
    COALESCE(
        m.naturalid,
        substring(c.preamble from 'MOT-\d+-\d+'),
        substring(c.name from 'MOT-\d+-\d+')
    ) AS motion_natural_id,
    m.motionid,
    m.name AS motion_name,
    m.status AS motion_status,
    c.name AS contract_name,
    c.status AS contract_status,
    c.date AS contract_date
FROM contracts c
LEFT JOIN planet_motions m ON (
    m.naturalid = COALESCE(
        substring(c.preamble from 'MOT-\d+-\d+'),
        substring(c.name from 'MOT-\d+-\d+')
    )
)
LEFT JOIN planets p ON p.admincenterid = COALESCE(m.admincenterid, c.partnerid)
WHERE (c.preamble ~ 'MOT-\d+-\d+' OR c.name ~ 'MOT-\d+-\d+')
ORDER BY c.id, c.date DESC;





