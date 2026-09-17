# converters/planet_motions.py
from datetime import datetime, timezone
from typing import Any, Dict, List


def convert_planet_motion(raw_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Converts a single raw motion payload into structured records for:
    - 'planet_motions'
    - 'planet_motion_votes'
    - 'planet_motion_components'
    """
    working_data = raw_data.get("payload", raw_data)
    if not isinstance(working_data, dict):
        return {
            "planet_motions": [],
            "planet_motion_votes": [],
            "planet_motion_components": [],
        }

    admin_center_id = working_data.get("adminCenterId")
    motion_id = working_data.get("id")
    if not motion_id:
        return {
            "planet_motions": [],
            "planet_motion_votes": [],
            "planet_motion_components": [],
        }

    creator = working_data.get("creator") or {}
    created_ts = (working_data.get("created") or {}).get("timestamp")
    v_start_ts = (working_data.get("votingStart") or {}).get("timestamp")
    v_end_ts = (working_data.get("votingEnd") or {}).get("timestamp")

    motions_list = [{
        "motionid": motion_id,
        "admincenterid": admin_center_id,
        "naturalid": working_data.get("naturalId"),
        "name": working_data.get("name"),
        "creator_id": creator.get("id"),
        "creator_username": creator.get("username"),
        "created_at": datetime.fromtimestamp(created_ts / 1000, tz=timezone.utc) if created_ts else None,
        "status": working_data.get("status"),
        "voting_start": datetime.fromtimestamp(v_start_ts / 1000, tz=timezone.utc) if v_start_ts else None,
        "voting_end": datetime.fromtimestamp(v_end_ts / 1000, tz=timezone.utc) if v_end_ts else None,
    }]

    votes_list = []
    for v in working_data.get("votes", []):
        if not isinstance(v, dict):
            continue
        voter = v.get("voter") or {}
        voted_ts = (v.get("voted") or {}).get("timestamp")

        votes_list.append({
            "motionid": motion_id,
            "voter_id": voter.get("id"),
            "voter_username": voter.get("username"),
            "role": v.get("role"),
            "status": v.get("status"),
            "voted_at": datetime.fromtimestamp(voted_ts / 1000, tz=timezone.utc) if voted_ts else None,
        })

    components_list = []
    for comp in working_data.get("components", []):
        if not isinstance(comp, dict):
            continue
        comp_id = comp.get("componentId")
        if not comp_id:
            continue

        contributor = comp.get("contributor") or {}
        recipient = comp.get("recipient") or {}
        amount_obj = comp.get("amount") or comp.get("costs") or {}
        amount_val = amount_obj.get("amount") if isinstance(amount_obj, dict) else comp.get("amount")

        components_list.append({
            "componentid": comp_id,
            "motionid": motion_id,
            "type": comp.get("type"),
            "contributor_id": contributor.get("id"),
            "contributor_username": contributor.get("username"),
            "recipient_id": recipient.get("id"),
            "recipient_username": recipient.get("username"),
            "amount": float(amount_val) if amount_val is not None and isinstance(amount_val, (int, float)) else None,
            "currency": amount_obj.get("currency") if isinstance(amount_obj, dict) else None,
            "program": comp.get("program"),
            "category": comp.get("category"),
        })

    return {
        "planet_motions": motions_list,
        "planet_motion_votes": votes_list,
        "planet_motion_components": components_list,
    }


def convert_planet_motions(raw_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Converts a list or batch of raw motion payloads into structured records for:
    - 'planet_motions'
    - 'planet_motion_votes'
    - 'planet_motion_components'
    """
    working_data = raw_data.get("payload", raw_data)

    raw_motions: List[Dict[str, Any]] = []
    admin_center_id = None

    if isinstance(working_data, dict):
        admin_center_id = working_data.get("adminCenterId")
        if "motions" in working_data and isinstance(working_data["motions"], list):
            raw_motions = working_data["motions"]
        elif working_data.get("id"):
            raw_motions = [working_data]
    elif isinstance(working_data, list):
        raw_motions = working_data

    motions_list: List[Dict[str, Any]] = []
    votes_list: List[Dict[str, Any]] = []
    components_list: List[Dict[str, Any]] = []

    for motion in raw_motions:
        if not isinstance(motion, dict):
            continue
        motion_id = motion.get("id")
        if not motion_id:
            continue

        creator = motion.get("creator") or {}
        created_ts = (motion.get("created") or {}).get("timestamp")
        v_start_ts = (motion.get("votingStart") or {}).get("timestamp")
        v_end_ts = (motion.get("votingEnd") or {}).get("timestamp")

        # 1. Main Motion Record
        motions_list.append({
            "motionid": motion_id,
            "admincenterid": admin_center_id,
            "naturalid": motion.get("naturalId"),
            "name": motion.get("name"),
            "creator_id": creator.get("id"),
            "creator_username": creator.get("username"),
            "created_at": datetime.fromtimestamp(created_ts / 1000, tz=timezone.utc) if created_ts else None,
            "status": motion.get("status"),
            "voting_start": datetime.fromtimestamp(v_start_ts / 1000, tz=timezone.utc) if v_start_ts else None,
            "voting_end": datetime.fromtimestamp(v_end_ts / 1000, tz=timezone.utc) if v_end_ts else None,
        })

        # 2. Votes
        for v in motion.get("votes", []):
            if not isinstance(v, dict):
                continue
            voter = v.get("voter") or {}
            voted_ts = (v.get("voted") or {}).get("timestamp")

            votes_list.append({
                "motionid": motion_id,
                "voter_id": voter.get("id"),
                "voter_username": voter.get("username"),
                "role": v.get("role"),
                "status": v.get("status"),
                "voted_at": datetime.fromtimestamp(voted_ts / 1000, tz=timezone.utc) if voted_ts else None,
            })

        # 3. Components
        for comp in motion.get("components", []):
            if not isinstance(comp, dict):
                continue
            comp_id = comp.get("componentId")
            if not comp_id:
                continue

            contributor = comp.get("contributor") or {}
            recipient = comp.get("recipient") or {}
            amount_obj = comp.get("amount") or comp.get("costs") or {}
            amount_val = amount_obj.get("amount") if isinstance(amount_obj, dict) else comp.get("amount")

            components_list.append({
                "componentid": comp_id,
                "motionid": motion_id,
                "type": comp.get("type"),
                "contributor_id": contributor.get("id"),
                "contributor_username": contributor.get("username"),
                "recipient_id": recipient.get("id"),
                "recipient_username": recipient.get("username"),
                "amount": float(amount_val) if amount_val is not None and isinstance(amount_val, (int, float)) else None,
                "currency": amount_obj.get("currency") if isinstance(amount_obj, dict) else None,
                "program": comp.get("program"),
                "category": comp.get("category"),
            })

    return {
        "planet_motions": motions_list,
        "planet_motion_votes": votes_list,
        "planet_motion_components": components_list,
    }
