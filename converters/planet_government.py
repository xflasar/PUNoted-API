# converters/planet_government.py
from datetime import datetime, timezone
from typing import Any, Dict, List
from converters.planet_motions import convert_planet_motion, convert_planet_motions


def convert_planet_government_term(raw_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Converts a single raw planet government term payload (e.g. admincenters/{adminCenterId}/terms/{termId})
    into structured records for 'planet_government_terms' and 'planet_government_candidates',
    and cascades any embedded 'motions' into motion records.
    """
    working_data = raw_data.get("payload", raw_data)
    if not isinstance(working_data, dict):
        return {
            "gov_terms": [],
            "gov_candidates": [],
            "planet_motions": [],
            "planet_motion_votes": [],
            "planet_motion_components": [],
        }

    term_id = working_data.get("id")
    admin_center_id = working_data.get("adminCenterId")

    if not term_id or not admin_center_id:
        return {
            "gov_terms": [],
            "gov_candidates": [],
            "planet_motions": [],
            "planet_motion_votes": [],
            "planet_motion_components": [],
        }

    elec_start = (working_data.get("electionStart") or {}).get("timestamp")
    elec_end = (working_data.get("electionEnd") or {}).get("timestamp")
    term_start = (working_data.get("start") or {}).get("timestamp")
    term_end = (working_data.get("end") or {}).get("timestamp")

    terms = [{
        "termid": term_id,
        "admincenterid": admin_center_id,
        "planet_natural_id": working_data.get("naturalId"),
        "election_start": datetime.fromtimestamp(elec_start / 1000, tz=timezone.utc) if elec_start else None,
        "election_end": datetime.fromtimestamp(elec_end / 1000, tz=timezone.utc) if elec_end else None,
        "term_start": datetime.fromtimestamp(term_start / 1000, tz=timezone.utc) if term_start else None,
        "term_end": datetime.fromtimestamp(term_end / 1000, tz=timezone.utc) if term_end else None,
        "parliament_size": working_data.get("parliamentSize"),
        "election_ongoing": working_data.get("electionOngoing", False),
    }]

    winners_ids = {w.get("id") for w in working_data.get("winners", []) if isinstance(w, dict)}
    candidates = []
    for cand in working_data.get("candidates", []):
        if not isinstance(cand, dict):
            continue
        user = cand.get("user") or {}
        corp = cand.get("corporation") or {}
        country = cand.get("country") or {}
        start_run = (cand.get("startOfRun") or {}).get("timestamp")

        cand_user_id = user.get("id")
        candidates.append({
            "termid": term_id,
            "userid": cand_user_id,
            "username": user.get("username"),
            "corporation_id": corp.get("id"),
            "corporation_name": corp.get("name"),
            "corporation_code": corp.get("code"),
            "country_id": country.get("id"),
            "country_code": country.get("code"),
            "country_name": country.get("name"),
            "votes": cand.get("votes"),
            "votes_percentage": cand.get("votesPercentage"),
            "is_winner": cand_user_id in winners_ids if cand_user_id else False,
            "start_of_run": datetime.fromtimestamp(start_run / 1000, tz=timezone.utc) if start_run else None,
        })

    all_motions: List[Dict[str, Any]] = []
    all_votes: List[Dict[str, Any]] = []
    all_components: List[Dict[str, Any]] = []

    nested_motions = working_data.get("motions")
    if nested_motions and isinstance(nested_motions, list):
        converted_m = convert_planet_motions({
            "adminCenterId": admin_center_id,
            "motions": nested_motions
        })
        all_motions.extend(converted_m.get("planet_motions", []))
        all_votes.extend(converted_m.get("planet_motion_votes", []))
        all_components.extend(converted_m.get("planet_motion_components", []))

    return {
        "gov_terms": terms,
        "gov_candidates": candidates,
        "planet_motions": all_motions,
        "planet_motion_votes": all_votes,
        "planet_motion_components": all_components,
    }


def convert_planet_government_terms(raw_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Converts a list or batch of raw planet government term payloads (e.g. admincenters/{adminCenterId}/terms)
    into structured records.
    """
    working_data = raw_data.get("payload", raw_data)

    terms_raw: List[Dict[str, Any]] = []
    default_admin_center_id = None

    if isinstance(working_data, list):
        terms_raw = working_data
    elif isinstance(working_data, dict):
        default_admin_center_id = working_data.get("adminCenterId")
        if "terms" in working_data and isinstance(working_data["terms"], list):
            terms_raw = working_data["terms"]
        else:
            terms_raw = [working_data]

    terms: List[Dict[str, Any]] = []
    candidates: List[Dict[str, Any]] = []
    all_motions: List[Dict[str, Any]] = []
    all_votes: List[Dict[str, Any]] = []
    all_components: List[Dict[str, Any]] = []

    for term in terms_raw:
        if not isinstance(term, dict):
            continue

        term_id = term.get("id")
        admin_center_id = term.get("adminCenterId") or default_admin_center_id

        if not term_id or not admin_center_id:
            continue

        elec_start = (term.get("electionStart") or {}).get("timestamp")
        elec_end = (term.get("electionEnd") or {}).get("timestamp")
        term_start = (term.get("start") or {}).get("timestamp")
        term_end = (term.get("end") or {}).get("timestamp")

        terms.append({
            "termid": term_id,
            "admincenterid": admin_center_id,
            "planet_natural_id": term.get("naturalId"),
            "election_start": datetime.fromtimestamp(elec_start / 1000, tz=timezone.utc) if elec_start else None,
            "election_end": datetime.fromtimestamp(elec_end / 1000, tz=timezone.utc) if elec_end else None,
            "term_start": datetime.fromtimestamp(term_start / 1000, tz=timezone.utc) if term_start else None,
            "term_end": datetime.fromtimestamp(term_end / 1000, tz=timezone.utc) if term_end else None,
            "parliament_size": term.get("parliamentSize"),
            "election_ongoing": term.get("electionOngoing", False),
        })

        winners_ids = {w.get("id") for w in term.get("winners", []) if isinstance(w, dict)}
        for cand in term.get("candidates", []):
            if not isinstance(cand, dict):
                continue
            user = cand.get("user") or {}
            corp = cand.get("corporation") or {}
            country = cand.get("country") or {}
            start_run = (cand.get("startOfRun") or {}).get("timestamp")

            cand_user_id = user.get("id")
            candidates.append({
                "termid": term_id,
                "userid": cand_user_id,
                "username": user.get("username"),
                "corporation_id": corp.get("id"),
                "corporation_name": corp.get("name"),
                "corporation_code": corp.get("code"),
                "country_id": country.get("id"),
                "country_code": country.get("code"),
                "country_name": country.get("name"),
                "votes": cand.get("votes"),
                "votes_percentage": cand.get("votesPercentage"),
                "is_winner": cand_user_id in winners_ids if cand_user_id else False,
                "start_of_run": datetime.fromtimestamp(start_run / 1000, tz=timezone.utc) if start_run else None,
            })

        nested_motions = term.get("motions")
        if nested_motions and isinstance(nested_motions, list):
            converted_m = convert_planet_motions({
                "adminCenterId": admin_center_id,
                "motions": nested_motions
            })
            all_motions.extend(converted_m.get("planet_motions", []))
            all_votes.extend(converted_m.get("planet_motion_votes", []))
            all_components.extend(converted_m.get("planet_motion_components", []))

    return {
        "gov_terms": terms,
        "gov_candidates": candidates,
        "planet_motions": all_motions,
        "planet_motion_votes": all_votes,
        "planet_motion_components": all_components,
    }
