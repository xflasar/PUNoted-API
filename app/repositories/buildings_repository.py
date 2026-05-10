# app/repositories/buildings_repository.py
class BuildingsRepository:
    def __init__(self, db):
        self.db = db

    async def get_all_buildings(self):
        query = """
            SELECT buildingid, ticker, name, type, area, expertisecategory
            FROM buildings
            WHERE type NOT IN ('INFRASTRUCTURE', 'CORE')
        """
        return await self.db.fetch_rows(query)

    async def get_all_workforce_capacities(self):
        query = """
            SELECT buildingid, workforcelevel, capacity, ishabitation 
            FROM building_workforce_capacities
        """
        return await self.db.fetch_rows(query)

    async def get_all_build_materials(self):
        query = """
            SELECT bbm.buildingid, bbm.amount, m.ticker 
            FROM building_build_materials bbm
            JOIN materials m ON bbm.materialid = m.materialid
        """
        return await self.db.fetch_rows(query)
