# app/repositories/materials_repository.py
class MaterialsRepository:
    def __init__(self, db):
        self.db = db

    async def get_all_materials(self):
        query = """
            SELECT materialid, ticker, name, resource 
            FROM materials
        """
        return await self.db.fetch_rows(query)

    async def get_all_processes(self):
        query = """
            SELECT mp.processid, mp.durationmillis, b.ticker as madein_ticker
            FROM material_processes mp
            LEFT JOIN buildings b ON mp.reactorid = b.buildingid
        """
        return await self.db.fetch_rows(query)

    async def get_all_process_io(self):
        query = """
            SELECT pio.processid, pio.iotype, pio.amount, m.ticker 
            FROM process_material_io pio
            JOIN materials m ON pio.materialid = m.materialid
        """
        return await self.db.fetch_rows(query)
