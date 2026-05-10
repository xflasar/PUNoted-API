from collections import defaultdict

from app.schemas.internal_planner import InputRecipeDTO, InternalMaterialDTO, RecipeIODTO


class MaterialsService:
    def __init__(self, repository):
        self.repo = repository

    async def get_planner_materials(self) -> list[InternalMaterialDTO]:
        materials_raw = await self.repo.get_all_materials() or []
        processes_raw = await self.repo.get_all_processes() or []
        io_raw = await self.repo.get_all_process_io() or []

        processes_map = {}
        for p in processes_raw:
            pid = str(p['processid'])
            processes_map[pid] = {
                "processid": pid,
                "name": f"Process {pid[:6]}",
                "durationmillis": p['durationmillis'] or 1000,
                "madeIn": p['madein_ticker'],
                "inputs": [],
                "outputs": []
            }

        for io in io_raw:
            pid = str(io['processid'])
            if pid in processes_map:
                io_obj = RecipeIODTO(ticker=io['ticker'], amount=float(io['amount']))
                if str(io['iotype']).upper() == 'INPUT':
                    processes_map[pid]['inputs'].append(io_obj)
                elif str(io['iotype']).upper() == 'OUTPUT':
                    processes_map[pid]['outputs'].append(io_obj)

        material_input_recipes = defaultdict(list)
        material_required_for = defaultdict(list)
        material_primary_building = {}

        for pid, proc in processes_map.items():
            primary_out = proc['outputs'][0].ticker if proc['outputs'] else "Unknown"
            proc['name'] = f"Make {primary_out}"

            recipe_dto = InputRecipeDTO(**proc)

            for out in proc['outputs']:
                ticker = out.ticker
                material_input_recipes[ticker].append(recipe_dto)
                if ticker not in material_primary_building:
                    material_primary_building[ticker] = proc['madeIn']

            for inp in proc['inputs']:
                ticker = inp.ticker
                material_required_for[ticker].append(pid)

        dtos = []
        for m in materials_raw:
            ticker = m['ticker']
            is_resource = bool(m['resource'])

            if not ticker:
                continue

            recipes = material_input_recipes.get(ticker, [])

            # --- SYNTHETIC RECIPE FOR STARTER NODES ---
            if is_resource and len(recipes) == 0:

                # Determine the building type based on material category
                cat = (m.get('category') or "").upper()
                bldg = "EXT" # Default solid extractor
                if "LIQUID" in cat:
                    bldg = "RIG"
                elif "GAS" in cat:
                    bldg = "COL"

                if ticker not in material_primary_building:
                    material_primary_building[ticker] = bldg

                synthetic_recipe = InputRecipeDTO(
                    processid=f"ext-{ticker}",
                    name=f"Extract {m['name'] or ticker}",
                    durationmillis=86400000,
                    madeIn=bldg,
                    inputs=[],
                    outputs=[RecipeIODTO(ticker=ticker, amount=100.0)]
                )
                recipes.append(synthetic_recipe)

                # Guess the building based on category if you have it (Solid->EXT, Liquid->RIG, Gas->COL).
                # If not, leave it None and let the user pick in the Base Manager.
                if ticker not in material_primary_building:
                    cat = (m.get('category') or "").upper()
                    if "LIQUID" in cat:
                        material_primary_building[ticker] = "RIG"
                    elif "GAS" in cat:
                        material_primary_building[ticker] = "COL"
                    else:
                        material_primary_building[ticker] = "EXT"

            dtos.append(InternalMaterialDTO(
                ticker=ticker,
                name=m['name'] or ticker,
                resource=is_resource,
                inputRecipes=recipes,
                requiredFor=material_required_for.get(ticker, []),
                production_building=material_primary_building.get(ticker)
            ))

        return dtos
