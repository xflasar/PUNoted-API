# converters/sites.py
import datetime
from datetime import datetime
from typing import Any, Dict, List

def convert_site_data(raw_records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Converts raw data to match the 'sites' table schema."""
    converted_record = {}

    record = raw_records["payload"]
    platforms = []
    building_options = []
    buildingOptionsIds = []

    build_options_obj = record.get("buildOptions") or {}
    for build_option in (build_options_obj.get("options") or []):
        if not isinstance(build_option, dict):
            continue
        build_option_materials = []

        materials_obj = build_option.get("materials") or {}
        for material in (materials_obj.get("quantities") or []):
            if not isinstance(material, dict):
                continue
            mat = material.get("material") or {}
            build_option_materials.append(
                {
                    "buildingid": build_option.get("id"),
                    "materialid": mat.get("id"),
                    "amount": material.get("amount"),
                }
            )

        build_option_workforce_capacities = []
        for workfoce_capacity in (build_option.get("workforceCapacities") or []):
            if not isinstance(workfoce_capacity, dict):
                continue
            build_option_workforce_capacities.append(
                {
                    "buildingid": build_option.get("id"),
                    "workforcelevel": workfoce_capacity.get("level"),
                    "capacity": workfoce_capacity.get("capacity"),
                }
            )

        buildingOptionsIds.append(build_option.get("id"))

        building_options.append(
            {
                "buildingid": build_option.get("id"),
                "name": build_option.get("name"),
                "ticker": build_option.get("ticker"),
                "type": build_option.get("type"),
                "area": build_option.get("area"),
                "expertisecategory": build_option.get("expertiseCategory"),
                "needsfertilesoil": build_option.get("needsFertileSoil"),
                "materials": build_option_materials,
                "workforcecapacities": build_option_workforce_capacities,
            }
        )

    for platform in (record.get("platforms") or []):
        if not isinstance(platform, dict):
            continue
        reclaimable_materials = []

        for material in (platform.get("reclaimableMaterials") or []):
            if not isinstance(material, dict):
                continue
            mat = material.get("material") or {}
            reclaimable_materials.append(
                {
                    "platformid": (platform.get("id") or "").replace("\x00", ""),
                    "materialid": mat.get("id"),
                    "amount": material.get("amount"),
                    "materialtype": "reclaimable",
                }
            )

        repair_materials = []
        for material in (platform.get("repairMaterials") or []):
            if not isinstance(material, dict):
                continue
            mat = material.get("material") or {}
            repair_materials.append(
                {
                    "platformid": (platform.get("id") or "").replace("\x00", ""),
                    "materialid": mat.get("id"),
                    "amount": material.get("amount"),
                    "materialtype": "repair",
                }
            )

        creation_time = platform.get("creationTime")
        if creation_time is not None and creation_time.get("timestamp") is not None:
            creation_time = datetime.fromtimestamp(creation_time["timestamp"] / 1000)
        else:
            creation_time = None

        last_repair = platform.get("lastRepair")
        if last_repair is not None and last_repair.get("timestamp") is not None:
            last_repair = datetime.fromtimestamp(last_repair["timestamp"] / 1000)
        else:
            last_repair = None

        book_val = platform.get("bookValue") or {}
        module_obj = platform.get("module") or {}

        platforms.append(
            {
                "platformid": (platform.get("id") or "").replace("\x00", ""),
                "siteid": platform.get("siteId"),
                "creationtime": creation_time,
                "bookvalueamount": book_val.get("amount"),
                "bookvaluecurrency": book_val.get("currency"),
                "area": platform.get("area"),
                "condition": platform.get("condition"),
                "buildingid": module_obj.get("reactorId"),
                "lastrepair": last_repair,
                "reclaimable_materials": reclaimable_materials,
                "repair_materials": repair_materials,
            }
        )

    founded_timestamp = record.get("founded")
    if founded_timestamp is not None and founded_timestamp.get("timestamp") is not None:
        founded_timestamp = datetime.fromtimestamp(founded_timestamp["timestamp"] / 1000)
    else:
        founded_timestamp = None

    addr = record.get("address") or {}
    addr_lines = addr.get("lines") or []
    sys_id = None
    pla_id = None
    if len(addr_lines) > 0 and isinstance(addr_lines[0], dict):
        sys_id = (addr_lines[0].get("entity") or {}).get("id")
    if len(addr_lines) > 1 and isinstance(addr_lines[1], dict):
        pla_id = (addr_lines[1].get("entity") or {}).get("id")

    converted_record = {
        "siteid": record.get("siteId"),
        "addresssystemid": sys_id,
        "addressplanetid": pla_id,
        "foundedtimestamp": founded_timestamp,
        "area": record.get("area"),
        "investedpermits": record.get("investedPermits"),
        "maximumpermits": record.get("maximumPermits"),
        "buildingoptions": buildingOptionsIds,
        "building_options": building_options,
        "platforms": platforms,
    }
    return converted_record


def convert_sites_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'sites' table schema."""
    converted_records = []
    records = []
    if raw_records["payload"].get("sites") is not None:
        records = raw_records["payload"]["sites"]
    elif raw_records["payload"].get("siteId") is not None:
        records = [raw_records["payload"]]

    for record in records:
        if not isinstance(record, dict):
            continue
        platforms = []
        building_options = []
        buildingOptionsIds = []

        build_options_obj = record.get("buildOptions") or {}
        for build_option in (build_options_obj.get("options") or []):
            if not isinstance(build_option, dict):
                continue
            build_option_materials = []

            materials_obj = build_option.get("materials") or {}
            for material in (materials_obj.get("quantities") or []):
                if not isinstance(material, dict):
                    continue
                mat = material.get("material") or {}
                build_option_materials.append(
                    {
                        "buildingid": build_option.get("id"),
                        "materialid": mat.get("id"),
                        "amount": material.get("amount"),
                    }
                )

            build_option_workforce_capacities = []
            for workfoce_capacity in (build_option.get("workforceCapacities") or []):
                if not isinstance(workfoce_capacity, dict):
                    continue
                build_option_workforce_capacities.append(
                    {
                        "buildingid": build_option.get("id"),
                        "workforcelevel": workfoce_capacity.get("level"),
                        "capacity": workfoce_capacity.get("capacity"),
                    }
                )

            buildingOptionsIds.append(build_option.get("id"))

            building_options.append(
                {
                    "buildingid": build_option.get("id"),
                    "name": build_option.get("name"),
                    "ticker": build_option.get("ticker"),
                    "type": build_option.get("type"),
                    "area": build_option.get("area"),
                    "expertisecategory": build_option.get("expertiseCategory"),
                    "needsfertilesoil": build_option.get("needsFertileSoil"),
                    "materials": build_option_materials,
                    "workforcecapacities": build_option_workforce_capacities,
                }
            )

        for platform in (record.get("platforms") or []):
            if not isinstance(platform, dict):
                continue
            reclaimable_materials = []

            for material in (platform.get("reclaimableMaterials") or []):
                if not isinstance(material, dict):
                    continue
                mat = material.get("material") or {}
                reclaimable_materials.append(
                    {
                        "platformid": (platform.get("id") or "").replace("\x00", ""),
                        "materialid": mat.get("id"),
                        "amount": material.get("amount"),
                        "materialtype": "reclaimable",
                    }
                )

            repair_materials = []
            for material in (platform.get("repairMaterials") or []):
                if not isinstance(material, dict):
                    continue
                mat = material.get("material") or {}
                repair_materials.append(
                    {
                        "platformid": (platform.get("id") or "").replace("\x00", ""),
                        "materialid": mat.get("id"),
                        "amount": material.get("amount"),
                        "materialtype": "repair",
                    }
                )

            creation_time = platform.get("creationTime")
            if creation_time is not None and creation_time.get("timestamp") is not None:
                creation_time = datetime.fromtimestamp(creation_time["timestamp"] / 1000)
            else:
                creation_time = None

            last_repair = platform.get("lastRepair")
            if last_repair is not None and last_repair.get("timestamp") is not None:
                last_repair = datetime.fromtimestamp(last_repair["timestamp"] / 1000)
            else:
                last_repair = None

            book_val = platform.get("bookValue") or {}
            module_obj = platform.get("module") or {}

            platforms.append(
                {
                    "platformid": (platform.get("id") or "").replace("\x00", ""),
                    "siteid": platform.get("siteId"),
                    "creationtime": creation_time,
                    "bookvalueamount": book_val.get("amount"),
                    "bookvaluecurrency": book_val.get("currency"),
                    "area": platform.get("area"),
                    "condition": platform.get("condition"),
                    "buildingid": module_obj.get("reactorId"),
                    "lastrepair": last_repair,
                    "reclaimable_materials": reclaimable_materials,
                    "repair_materials": repair_materials,
                }
            )

        founded_timestamp = record.get("founded")
        if founded_timestamp is not None and founded_timestamp.get("timestamp") is not None:
            founded_timestamp = datetime.fromtimestamp(founded_timestamp["timestamp"] / 1000)
        else:
            founded_timestamp = None

        addr = record.get("address") or {}
        addr_lines = addr.get("lines") or []
        sys_id = None
        pla_id = None
        if len(addr_lines) > 0 and isinstance(addr_lines[0], dict):
            sys_id = (addr_lines[0].get("entity") or {}).get("id")
        if len(addr_lines) > 1 and isinstance(addr_lines[1], dict):
            pla_id = (addr_lines[1].get("entity") or {}).get("id")

        converted_records.append(
            {
                "siteid": record.get("siteId"),
                "addresssystemid": sys_id,
                "addressplanetid": pla_id,
                "foundedtimestamp": founded_timestamp,
                "area": record.get("area"),
                "investedpermits": record.get("investedPermits"),
                "maximumpermits": record.get("maximumPermits"),
                "buildingoptions": buildingOptionsIds,
                "building_options": building_options,
                "platforms": platforms,
            }
        )
    return converted_records


def convert_site_platforms_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'site_platforms' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "id": record.get("id"),
                "siteId": record.get("siteId"),
                "buildingPlatformId": record.get("buildingPlatformId"),
                "area": record.get("area"),
                "creationTimestamp": record.get("creationTimestamp"),
                "bookValueAmount": record.get("bookValueAmount"),
                "bookValueCurrency": record.get("bookValueCurrency"),
                "condition": record.get("condition"),
                "lastRepairTimestamp": record.get("lastRepairTimestamp"),
            }
        )
    return converted_records


def convert_site_experts_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'site_experts' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "siteId": record.get("siteId"),
                "category": record.get("category"),
                "current": record.get("current"),
                "limit": record.get("limit"),
                "available": record.get("available"),
                "efficiencyGain": record.get("efficiencyGain"),
                "progress": record.get("progress"),
            }
        )
    return converted_records


def convert_site_platform_updated(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Converts raw payload from SITE_PLATFORM_UPDATED message.
    Payload contains a single platform object for a specific site.
    """
    platform = raw_record.get("payload") if isinstance(raw_record, dict) else {}
    if not isinstance(platform, dict):
        platform = {}

    platform_id = (platform.get("id") or "").replace("\x00", "")
    site_id = platform.get("siteId")

    reclaimable_materials = []
    for material in (platform.get("reclaimableMaterials") or []):
        if not isinstance(material, dict):
            continue
        mat = material.get("material") or {}
        reclaimable_materials.append(
            {
                "platformid": platform_id,
                "materialid": mat.get("id"),
                "amount": material.get("amount"),
                "materialtype": "reclaimable",
            }
        )

    repair_materials = []
    for material in (platform.get("repairMaterials") or []):
        if not isinstance(material, dict):
            continue
        mat = material.get("material") or {}
        repair_materials.append(
            {
                "platformid": platform_id,
                "materialid": mat.get("id"),
                "amount": material.get("amount"),
                "materialtype": "repair",
            }
        )

    creation_time = platform.get("creationTime")
    if creation_time is not None and isinstance(creation_time, dict) and creation_time.get("timestamp") is not None:
        creation_time = datetime.fromtimestamp(creation_time["timestamp"] / 1000)
    else:
        creation_time = None

    last_repair = platform.get("lastRepair")
    if last_repair is not None and isinstance(last_repair, dict) and last_repair.get("timestamp") is not None:
        last_repair = datetime.fromtimestamp(last_repair["timestamp"] / 1000)
    else:
        last_repair = None

    book_val = platform.get("bookValue") or {}
    module_obj = platform.get("module") or {}

    converted_platform = {
        "platformid": platform_id,
        "siteid": site_id,
        "creationtime": creation_time,
        "bookvalueamount": book_val.get("amount"),
        "bookvaluecurrency": book_val.get("currency"),
        "area": platform.get("area"),
        "condition": platform.get("condition"),
        "buildingid": module_obj.get("reactorId"),
        "lastrepair": last_repair,
        "reclaimable_materials": reclaimable_materials,
        "repair_materials": repair_materials,
    }

    return {
        "siteid": site_id,
        "platform": converted_platform,
    }

