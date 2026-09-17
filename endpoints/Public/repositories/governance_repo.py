import json
from typing import Optional, List, Dict, Any

async def fetch_planet_government_terms(db, planet: Optional[str] = None, termid: Optional[str] = None) -> List[Dict[str, Any]]:
    query = """
    SELECT 
        t.termid,
        t.admincenterid,
        t.planet_natural_id,
        p.name AS planet_name,
        p.naturalid AS planet_natural_id_code,
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
    LEFT JOIN planets p ON (p.admincenterid = t.admincenterid OR p.naturalid = t.admincenterid)
    LEFT JOIN planet_government_candidates c ON c.termid = t.termid
    WHERE 1=1
    """
    args = []
    if termid:
        args.append(termid)
        query += f" AND t.termid = ${len(args)}"
    if planet:
        args.append(planet.upper())
        query += f" AND (UPPER(p.naturalid) = ${len(args)} OR UPPER(p.name) = ${len(args)} OR UPPER(t.admincenterid) = ${len(args)})"

    query += """
    GROUP BY t.termid, t.admincenterid, t.planet_natural_id, p.name, p.naturalid, 
             t.term_start, t.term_end, t.election_start, t.election_end, t.parliament_size, t.election_ongoing
    ORDER BY t.term_start DESC
    """
    records = await db.fetch_rows(query, *args)
    results = []
    for r in records:
        d = dict(r)
        if isinstance(d.get("candidates"), str):
            d["candidates"] = json.loads(d["candidates"])
        results.append(d)
    return results


async def fetch_planet_motions(
    db, 
    planet: Optional[str] = None, 
    motionid: Optional[str] = None, 
    status: Optional[str] = None,
    full: bool = False
) -> List[Dict[str, Any]]:
    if not full:
        query = """
        SELECT 
            m.motionid,
            m.admincenterid,
            m.naturalid,
            p.name AS planet_name,
            p.naturalid AS planet_natural_id_code,
            m.name AS motion_name,
            m.status,
            m.creator_id,
            m.creator_username,
            m.created_at,
            m.voting_start,
            m.voting_end
        FROM planet_motions m
        LEFT JOIN planets p ON (p.admincenterid = m.admincenterid OR p.naturalid = m.naturalid)
        WHERE 1=1
        """
        args = []
        if motionid:
            args.append(motionid)
            query += f" AND m.motionid = ${len(args)}"
        if planet:
            args.append(planet.upper())
            query += f" AND (UPPER(p.naturalid) = ${len(args)} OR UPPER(p.name) = ${len(args)} OR UPPER(m.naturalid) = ${len(args)} OR UPPER(m.admincenterid) = ${len(args)})"
        if status:
            args.append(status.upper())
            query += f" AND UPPER(m.status) = ${len(args)}"

        query += " ORDER BY m.created_at DESC"
        records = await db.fetch_rows(query, *args)
        return [dict(r) for r in records]

    query = """
    SELECT 
        m.motionid,
        m.admincenterid,
        m.naturalid,
        p.name AS planet_name,
        p.naturalid AS planet_natural_id_code,
        m.name AS motion_name,
        m.status,
        m.creator_id,
        m.creator_username,
        m.created_at,
        m.voting_start,
        m.voting_end,
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
                'recipient_username', comp.recipient_username,
                'amount', comp.amount,
                'currency', comp.currency,
                'program', comp.program,
                'category', comp.category
            )) FILTER (WHERE comp.componentid IS NOT NULL), '[]'::json
        ) AS components
    FROM planet_motions m
    LEFT JOIN planets p ON (p.admincenterid = m.admincenterid OR p.naturalid = m.naturalid)
    LEFT JOIN planet_motion_votes v ON v.motionid = m.motionid
    LEFT JOIN planet_motion_components comp ON comp.motionid = m.motionid
    WHERE 1=1
    """
    args = []
    if motionid:
        args.append(motionid)
        query += f" AND m.motionid = ${len(args)}"
    if planet:
        args.append(planet.upper())
        query += f" AND (UPPER(p.naturalid) = ${len(args)} OR UPPER(p.name) = ${len(args)} OR UPPER(m.naturalid) = ${len(args)} OR UPPER(m.admincenterid) = ${len(args)})"
    if status:
        args.append(status.upper())
        query += f" AND UPPER(m.status) = ${len(args)}"

    query += """
    GROUP BY m.motionid, m.admincenterid, m.naturalid, p.name, p.naturalid, m.name, m.status, 
             m.creator_id, m.creator_username, m.created_at, m.voting_start, m.voting_end
    ORDER BY m.created_at DESC
    """
    records = await db.fetch_rows(query, *args)
    results = []
    for r in records:
        d = dict(r)
        if isinstance(d.get("votes"), str):
            d["votes"] = json.loads(d["votes"])
        if isinstance(d.get("components"), str):
            d["components"] = json.loads(d["components"])
        results.append(d)
    return results
