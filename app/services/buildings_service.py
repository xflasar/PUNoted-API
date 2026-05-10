from collections import defaultdict

from app.schemas.internal_planner import BuildingRequirementDTO, InternalBuildingDTO


class BuildingsService:
    def __init__(self, repository):
        self.repo = repository

    async def get_planner_buildings(self) -> list[InternalBuildingDTO]:
        buildings_raw = await self.repo.get_all_buildings() or []
        capacities_raw = await self.repo.get_all_workforce_capacities() or []
        materials_raw = await self.repo.get_all_build_materials() or []

        workers_map = defaultdict(dict)
        supply_map = defaultdict(dict)

        for cap in capacities_raw:
            b_id = cap['buildingid']
            level = cap['workforcelevel']
            amount = cap['capacity']

            if cap['ishabitation']:
                supply_map[b_id][level] = amount
            else:
                workers_map[b_id][level] = amount

        build_req_map = defaultdict(list)
        for mat in materials_raw:
            build_req_map[mat['buildingid']].append(
                BuildingRequirementDTO(ticker=mat['ticker'], amount=mat['amount'])
            )

        dtos = []
        for b in buildings_raw:
            b_id = b['buildingid']
            ticker = b['ticker']
            name = b['name'] or ""

            # --- EXACT DB TYPE MAPPING ---
            raw_db_type = (b['type'] or "").strip().upper()

            if raw_db_type in ["STORAGE", "HABITATION"]:
                ui_type = "infrastructure"
            else:
                ui_type = "production" # Captures MANUFACTURING, EXT, RIG, COL, etc.

            # --- INDEPENDENT STORAGE VARIABLES ---
            storage_weight = None
            storage_volume = None

            if raw_db_type == "STORAGE" or ticker in ["STO", "STA", "LST"]:
                if ticker == "STA":
                    storage_weight = 500
                    storage_volume = 500
                elif ticker == "STO":
                    storage_weight = 1000
                    storage_volume = 1000
                elif ticker == "LST":
                    storage_weight = 2000
                    storage_volume = 2000
                else:
                    storage_weight = 500
                    storage_volume = 500

            dto = InternalBuildingDTO(
                id=b_id,
                ticker=ticker,
                name=name,
                type=ui_type,
                area=b['area'] or 0,
                category=b.get('expertisecategory', None),
                buildReq=build_req_map.get(b_id, []),
                workers=workers_map.get(b_id) if b_id in workers_map else None,
                supply=supply_map.get(b_id) if b_id in supply_map else None,
                storageWeight=storage_weight,
                storageVolume=storage_volume
            )
            dtos.append(dto)

        return dtos
