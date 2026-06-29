import asyncio
import json
from typing import Any, Dict, Optional

# In-memory caches
LIVE_GROUP_STATES: Dict[str, Dict[str, Any]] = {}
LIVE_GROUP_SAVE_TASKS: Dict[str, asyncio.Task] = {}
SAVE_DEBOUNCE_TIME = 3.0


async def get_latest_chain_from_db(pool, group_id: str) -> Optional[Dict[str, Any]]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT chain_data FROM production_groups WHERE id = $1;", group_id)
        if row:
            try:
                return row["chain_data"]
            except json.JSONDecodeError:
                pass
    return None


async def save_group_chain_data(pool, group_id: str, chain_data: Dict[str, Any]):
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE production_groups SET chain_data = $1::jsonb, updated_at = NOW() WHERE id = $2",
            json.dumps(chain_data),
            group_id,
        )


async def debounce_save_group_chain_data(pool, group_id: str, current_chain: Dict[str, Any]):
    if group_id in LIVE_GROUP_SAVE_TASKS:
        LIVE_GROUP_SAVE_TASKS[group_id].cancel()
        del LIVE_GROUP_SAVE_TASKS[group_id]

    async def save_routine():
        try:
            await asyncio.sleep(SAVE_DEBOUNCE_TIME)
            await save_group_chain_data(pool, group_id, current_chain)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"Error saving group {group_id}: {e}")
        finally:
            if group_id in LIVE_GROUP_SAVE_TASKS:
                del LIVE_GROUP_SAVE_TASKS[group_id]

    LIVE_GROUP_SAVE_TASKS[group_id] = asyncio.create_task(save_routine())


async def apply_change_to_live_chain(pool, current_chain: Dict[str, Any], message: Dict[str, Any], group_id: str):
    """Applies an incremental change (like a node move) to the Chain object in memory."""

    if message["type"] == "NODE_MOVE":
        payload_item = message["payload"]
        node_id = payload_item.get("id")
        new_position = payload_item.get("position")

        if not node_id or not new_position:
            return
        for node in current_chain["nodes"]:
            chain_node_id = node.get("nodeId", "")

            if chain_node_id == node_id:
                if "x" in new_position and "y" in new_position:
                    node["position"] = {"x": new_position["x"], "y": new_position["y"]}
                    break

    elif message["type"] == "NODE_ADD":
        payload = message["payload"]
        current_chain["nodes"].append(payload["data"])
    elif message["type"] == "NODE_REMOVE":
        payload = message["payload"]
        node_id_to_remove = payload.get("nodeId")

        if not node_id_to_remove:
            return

        updated_nodes = [node for node in current_chain["nodes"] if node["nodeId"] != node_id_to_remove]

        current_chain["nodes"] = updated_nodes

        updated_links = [
            link
            for link in current_chain.get("links", [])
            if not (link.get("source", "") == node_id_to_remove or link.get("target", "") == node_id_to_remove)
        ]
        current_chain["links"] = updated_links
    elif message["type"] == "NODE_UPDATE":
        payload = message["payload"]["node"]

        payload_id = payload["nodeId"]

        updated_nodes = [
            payload if node.get("nodeId") == payload_id else node for node in current_chain.get("nodes", [])
        ]

        new_chain = {
            **current_chain,
            "nodes": updated_nodes,
        }

        current_chain = new_chain
    elif message["type"] == "EDGE_UPDATE":
        payload = message["payload"]

        if payload["type"] == "add":
            current_chain["links"].append(payload["item"])

        if payload["type"] == "remove":
            edge_id_to_remove = payload["id"]

            updated_links = [link for link in current_chain["links"] if link.get("id") != edge_id_to_remove]

            current_chain["links"] = updated_links
    LIVE_GROUP_STATES[group_id] = current_chain
    await debounce_save_group_chain_data(pool, group_id, current_chain)
