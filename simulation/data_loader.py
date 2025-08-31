from typing import Any, Dict, List, Optional, Tuple

from simulation.production_planner import MS_IN_DAY

from .data_models import (
    Material, WorkforceType, Building, RecipeInput, RecipeOutput, Recipe,
    ProductionOrder, BuildingInstance, StorageItem, PlanetWorkforce, Planet,
    CompanyHQ, Company, MarketData, BuildingCostItem, BuildingWorkforceRequirement,
    SimulationState, Site, SiteBuilding, BuildingWorkforce,
    PlanetData, Resource, BuildRequirement, ProductionFee, COGCProgram, COGCVote
)

# --- HELPER FUNCTION ---
def _parse_recipe_name_components(recipe_name_str: str) -> Tuple[str, Dict[str, int], Dict[str, int]]:
    """
    Parses a StandardRecipeName string into its building ticker,
    and dictionaries of input and output material amounts.
    Example: 'FP:20xH2O=>14xDW' -> 'FP', {'H2O': 20}, {'DW': 14}
    Example: 'RIG:=>' -> 'RIG', {}, {}
    Handles inputs/outputs separated by '-' or '+'.
    """
    if not isinstance(recipe_name_str, str):
        return "", {}, {}

    parts = recipe_name_str.split(':', 1)
    
    building_ticker = parts[0].strip()
    recipe_body = parts[1].strip() if len(parts) > 1 else ""

    io_parts = recipe_body.split('=>', 1)
    inputs_str = io_parts[0].strip() if len(io_parts) > 0 else ""
    outputs_str = io_parts[1].strip() if len(io_parts) > 1 else ""

    inputs = {}
    # Split by '-' for inputs as per your example '4xFLX-4xREA-1xTC'
    for item in inputs_str.split('-'):
        item = item.strip()
        if 'x' in item:
            amount, ticker = item.split('x', 1)
            try:
                inputs[ticker.strip()] = int(amount.strip())
            except ValueError:
                pass
    
    outputs = {}
    # Split by '-' for outputs as well, assuming consistency
    for item in outputs_str.split('-'):
        item = item.strip()
        if 'x' in item:
            amount, ticker = item.split('x', 1)
            try:
                outputs[ticker.strip()] = int(amount.strip())
            except ValueError:
                pass
    
    return building_ticker, inputs, outputs

# --- Helper to parse production orders ---
def _parse_production_orders(production_data: List[Dict[str, Any]], static_recipes: Dict[str, Recipe]) -> Dict[str, ProductionOrder]:
    """Parses raw production data into a dictionary of ProductionOrder objects."""
    parsed_orders = {}
    for line_data in production_data:
        for order_data in line_data.get("Orders", []):
            try:
                standard_recipe_name = order_data.get("StandardRecipeName")
                if not standard_recipe_name:
                    continue # Skip if no recipe name

                # Find the recipe definition based on StandardRecipeName
                # Assuming static_recipes is keyed by StandardRecipeName for direct lookup
                # If static_recipes is keyed by Ticker, you might need to iterate or create a reverse map
                recipe_def = static_recipes.get(standard_recipe_name)
                if not recipe_def:
                    continue

                parsed_orders[order_data["ProductionLineOrderId"]] = ProductionOrder(
                    ProductionLineOrderId=order_data["ProductionLineOrderId"],
                    BuildingId=order_data["BuildingId"],
                    StandardRecipeName=standard_recipe_name,
                    CreatedEpochMs=order_data["CreatedEpochMs"],
                    StartedEpochMs=order_data.get("StartedEpochMs"),
                    CompletionEpochMs=order_data.get("CompletionEpochMs"),
                    DurationMs=order_data["DurationMs"],
                    LastUpdatedEpochMs=order_data["LastUpdatedEpochMs"],
                    CompletedPercentage=order_data["CompletedPercentage"],
                    IsHalted=order_data["IsHalted"],
                    Recurring=order_data["Recurring"],
                    ProductionFee=order_data.get("ProductionFee"),
                    ProductionFeeCurrency=order_data.get("ProductionFeeCurrency"),
                )
            except KeyError as e:
                print(f"Warning: Skipping production order due to missing key {e} in {order_data}")
            except Exception as e:
                print(f"Warning: Error parsing production order {order_data}: {e}")
    return parsed_orders


def load_static_game_data(static_data_raw: Dict[str, Any]) -> Tuple[
    Dict[str, Material],
    Dict[str, WorkforceType],
    Dict[str, Building],
    Dict[str, Recipe],
    Dict[str, str], # static_building_name_to_ticker
    Dict[str, str], # static_normalized_recipe_to_base_recipe_ticker_map
    Dict[str, List[BuildingWorkforce]],
    List[PlanetData] # return type for all_planets_data
]:
    """
    Loads and transforms static game data (materials, buildings, recipes, etc.)
    from raw JSON data into dataclass instances for easier access.
    """
    static_materials: Dict[str, Material] = {}
    for material_data in static_data_raw.get("materials", {}):
        static_materials[material_data['Ticker']] = Material(
            MaterialId=material_data['MaterialId'],
            MaterialName=material_data['Name'],
            MaterialTicker=material_data['Ticker'],
            CategoryName=material_data['CategoryName'],
            CategoryId=material_data['CategoryId'],
            Weight=material_data['Weight'],
            Volume=material_data['Volume'],
            UserNameSubmitted=material_data.get('UserNameSubmitted'),
            Timestamp=material_data.get('Timestamp')
        )

    static_workforce_types: Dict[str, WorkforceType] = {}
    for wf_data in static_data_raw.get("workforce_types", []):
        static_workforce_types[wf_data['Name']] = WorkforceType(
            name=wf_data['Name'],
            description=wf_data.get('Description', '')
        )

    static_buildings: Dict[str, Building] = {}
    building_costs_map: Dict[str, List[RecipeInput]] = {}
    building_workforce_req_map: Dict[str, List[BuildingWorkforceRequirement]] = {}

    for cost_data in static_data_raw.get("building_costs", []):
        building_ticker = cost_data['Building']
        if building_ticker not in building_costs_map:
            building_costs_map[building_ticker] = []
        building_costs_map[building_ticker].append(RecipeInput(
            MaterialTicker=cost_data['Material'],
            Amount=cost_data['Amount']
        ))

    for wf_req_data in static_data_raw.get("building_workforce_requirements", []):
        building_ticker = wf_req_data['Building']
        if building_ticker not in building_workforce_req_map:
            building_workforce_req_map[building_ticker] = []
        building_workforce_req_map[building_ticker].append(BuildingWorkforceRequirement(
            workforce_type_name=wf_req_data['WorkforceType'],
            capacity_needed=wf_req_data['Amount']
        ))

    for bldg_data in static_data_raw.get("buildings", []):
        b_ticker = bldg_data['Ticker']
        static_buildings[b_ticker] = Building(
            ticker=b_ticker,
            name=bldg_data['Name'],
            area=bldg_data['Area'],
            expertise=bldg_data['Expertise']
        )
    
    static_recipes: Dict[str, Recipe] = {}
    for recipe_data in static_data_raw.get("recipes", []):
        building_ticker, inputs_dict, outputs_dict = _parse_recipe_name_components(recipe_data['StandardRecipeName'])
        static_recipes[recipe_data['StandardRecipeName']] = Recipe(
            RecipeName=recipe_data['RecipeName'],
            BuildingTicker=building_ticker,
            DurationMs=recipe_data.get('TimeMs', 0),
            StandardRecipeName=recipe_data['StandardRecipeName'],
            Inputs=[RecipeInput(MaterialTicker=k, Amount=v) for k, v in inputs_dict.items()],
            Outputs=[RecipeOutput(MaterialTicker=k, Amount=v) for k, v in outputs_dict.items()],
        )

    static_recipe_production_lines: Dict[str, str] = {}
    for recipe_data in static_data_raw.get("recipes", []):
        building_ticker, _, _ = _parse_recipe_name_components(recipe_data['StandardRecipeName'])
        if building_ticker:
            static_recipe_production_lines[recipe_data['StandardRecipeName']] = building_ticker

    # Populate static_normalized_recipe_to_base_recipe_ticker_map
    static_normalized_recipe_to_base_recipe_ticker_map: Dict[str, str] = {}
    for recipe_name, recipe_obj in static_recipes.items():
        building_ticker = recipe_obj.BuildingTicker
        if building_ticker:
            static_normalized_recipe_to_base_recipe_ticker_map[recipe_name] = building_ticker
        else:
            static_normalized_recipe_to_base_recipe_ticker_map[recipe_name] = recipe_name


    static_building_name_to_ticker: Dict[str, str] = {
        b.name.lower(): b.ticker for b in static_buildings.values()
    }

    # Load static building workforces from /rain/buildingworkforces
    static_building_workforces: Dict[str, List[BuildingWorkforce]] = {}
    for bwf_data in static_data_raw.get("building_workforces", []):
        building_ticker = bwf_data['Building']
        if building_ticker not in static_building_workforces:
            static_building_workforces[building_ticker] = []
        static_building_workforces[building_ticker].append(BuildingWorkforce(
            workforce_type_name=bwf_data['Level'],
            building_ticker=bwf_data['Building'],
            capacity_needed=bwf_data['Capacity']
        ))

    # --- Load all_planets_data ---
    all_planets_data: List[PlanetData] = []
    for planet_data_raw in static_data_raw.get("planets", []):
        resources = [Resource(MaterialId=r['MaterialId'], ResourceType=r['ResourceType'], Factor=r['Factor']) for r in planet_data_raw.get('Resources', [])]
        build_requirements = [BuildRequirement(
            MaterialName=br['MaterialName'],
            MaterialId=br['MaterialId'],
            MaterialTicker=br['MaterialTicker'],
            MaterialCategory=br['MaterialCategory'],
            MaterialAmount=br['MaterialAmount'],
            MaterialWeight=br['MaterialWeight'],
            MaterialVolume=br['MaterialVolume']
        ) for br in planet_data_raw.get('BuildRequirements', [])]
        production_fees = [ProductionFee(
            Category=pf['Category'],
            WorkforceLevel=pf['WorkforceLevel'],
            FeeAmount=pf['FeeAmount'],
            FeeCurrency=pf['FeeCurrency']
        ) for pf in planet_data_raw.get('ProductionFees', [])]
        cogc_programs = [COGCProgram(
            ProgramType=cp.get('ProgramType'),
            StartEpochMs=cp['StartEpochMs'],
            EndEpochMs=cp['EndEpochMs']
        ) for cp in planet_data_raw.get('COGCPrograms', [])]
        cogc_votes = [COGCVote(
            CompanyName=cv['CompanyName'],
            CompanyCode=cv['CompanyCode'],
            Influence=cv['Influence'],
            VoteType=cv['VoteType'],
            VoteTimeEpochMs=cv['VoteTimeEpochMs']
        ) for cv in planet_data_raw.get('COGCVotes', [])]

        all_planets_data.append(PlanetData(
            Resources=resources,
            BuildRequirements=build_requirements,
            ProductionFees=production_fees,
            COGCPrograms=cogc_programs,
            COGCVotes=cogc_votes,
            COGCUpkeep=planet_data_raw.get('COGCUpkeep', []),
            PlanetId=planet_data_raw['PlanetId'],
            PlanetNaturalId=planet_data_raw['PlanetNaturalId'],
            PlanetName=planet_data_raw['PlanetName'],
            Namer=planet_data_raw.get('Namer'),
            NamingDataEpochMs=planet_data_raw['NamingDataEpochMs'],
            Nameable=planet_data_raw['Nameable'],
            SystemId=planet_data_raw['SystemId'],
            Gravity=planet_data_raw['Gravity'],
            MagneticField=planet_data_raw['MagneticField'],
            Mass=planet_data_raw['Mass'],
            MassEarth=planet_data_raw['MassEarth'],
            OrbitSemiMajorAxis=planet_data_raw['OrbitSemiMajorAxis'],
            OrbitEccentricity=planet_data_raw['OrbitEccentricity'],
            OrbitInclination=planet_data_raw['OrbitInclination'],
            OrbitRightAscension=planet_data_raw['OrbitRightAscension'],
            OrbitPeriapsis=planet_data_raw['OrbitPeriapsis'],
            OrbitIndex=planet_data_raw['OrbitIndex'],
            Pressure=planet_data_raw['Pressure'],
            Radiation=planet_data_raw['Radiation'],
            Radius=planet_data_raw['Radius'],
            Sunlight=planet_data_raw['Sunlight'],
            Surface=planet_data_raw['Surface'],
            Temperature=planet_data_raw['Temperature'],
            Fertility=planet_data_raw['Fertility'],
            HasLocalMarket=planet_data_raw['HasLocalMarket'],
            HasChamberOfCommerce=planet_data_raw['HasChamberOfCommerce'],
            HasWarehouse=planet_data_raw['HasWarehouse'],
            HasAdministrationCenter=planet_data_raw['HasAdministrationCenter'],
            HasShipyard=planet_data_raw['HasShipyard'],
            FactionCode=planet_data_raw.get('FactionCode'),
            FactionName=planet_data_raw.get('FactionName'),
            GoverningEntity=planet_data_raw['GoverningEntity'],
            CurrencyName=planet_data_raw['CurrencyName'],
            CurrencyCode=planet_data_raw['CurrencyCode'],
            BaseLocalMarketFee=planet_data_raw['BaseLocalMarketFee'],
            LocalMarketFeeFactor=planet_data_raw['LocalMarketFeeFactor'],
            WarehouseFee=planet_data_raw['WarehouseFee'],
            EstablishmentFee=planet_data_raw['EstablishmentFee'],
            PopulationId=planet_data_raw['PopulationId'],
            COGCProgramStatus=planet_data_raw.get('COGCProgramStatus'),
            PlanetTier=planet_data_raw['PlanetTier'],
            UserNameSubmitted=planet_data_raw['UserNameSubmitted'],
            Timestamp=planet_data_raw['Timestamp']
        ))
    # --- END ---

    return (
        static_materials,
        static_workforce_types,
        static_buildings,
        static_recipes,
        static_building_name_to_ticker,
        static_normalized_recipe_to_base_recipe_ticker_map,
        static_building_workforces,
        all_planets_data # Return the all_planets_data
    )

# --- MAIN DATA LOADER FUNCTION ---
def load_initial_company_state(
    fio_dynamic_data: Dict[str, Any],
    fio_static_data: Tuple[
        Dict[str, Material],
        Dict[str, WorkforceType],
        Dict[str, Building],
        Dict[str, Recipe],
        Dict[str, str],
        Dict[str, str],
        Dict[str, List[BuildingWorkforce]],
        List[PlanetData] # Expecting PlanetData here
    ]
) -> SimulationState:
    """
    Loads and parses initial company state from FIO dynamic and static data.
    """
    # Unpack static data tuple
    static_materials, static_workforce_types, static_buildings, static_recipes, \
    static_building_name_to_ticker, static_normalized_recipe_to_base_recipe_ticker_map, \
    static_building_workforces, all_planets_data = fio_static_data


    # ----------------------------------------------------------------------
    # 1. Parse Dynamic Data
    # ----------------------------------------------------------------------

    # Company HQ (Handle missing data gracefully)
    hq_data = fio_dynamic_data.get('CompanyData', {}).get('Headquarter')
    
    if hq_data:
        hq = CompanyHQ(
            PlanetId=hq_data.get('PlanetId'),
            PlanetNaturalId=hq_data.get('PlanetNaturalId'),
            PlanetName=hq_data.get('PlanetName'),
            Tier=hq_data.get('Tier')
        )
    else:
        # If no HQ data, create a CompanyHQ with all None fields as requested
        print("Warning: Headquarter data not found in dynamic FIO data. Initializing CompanyHQ with default None values.")
        hq = CompanyHQ(
            PlanetId=None,
            PlanetNaturalId=None,
            PlanetName=None,
            Tier=None
        )

    company_name = fio_dynamic_data.get('CompanyData', {}).get('CompanyName', 'Unknown Company')
    company_cash = fio_dynamic_data.get('CompanyData', {}).get('Cash', 0.0)
    company_id = fio_dynamic_data.get('CompanyData', {}).get('CompanyId', 'UNKNOWN_COMPANY_ID')

    # Planets: Collect unique planet data from Sites and Workforce sections
    # Initialize planets_dict with all planets from all_planets_data
    planets_dict: Dict[str, PlanetData] = {}
    for planet_data in all_planets_data:
        planet_data.sites = {}
        planet_data.workforce = {}
        planets_dict[planet_data.PlanetId] = planet_data

    # Supplement/update planet data from Sites
    for site_dict in fio_dynamic_data.get("sites", []):
        planet_id = site_dict["PlanetId"]
        if planet_id in planets_dict: # Only add sites to planets that exist in all_planets_data
            # Prepare buildings for this site
            buildings_on_site: Dict[str, BuildingInstance] = {}
            for bldg_data in site_dict.get("Buildings", []):
                buildings_on_site[bldg_data["SiteBuildingId"]] = BuildingInstance(
                    SiteBuildingId=bldg_data["SiteBuildingId"],
                    BuildingTicker=bldg_data["BuildingTicker"],
                    BuildingId=bldg_data.get("BuildingId"),
                    BuildingCreated=bldg_data.get("BuildingCreated"),
                    BuildingName=bldg_data.get("BuildingName"),
                    BuildingLastRepair=bldg_data.get("BuildingLastRepair"),
                    Condition=bldg_data.get("Condition"),
                    ProductionLineIds=[] # Assuming empty list if not present in data
                )
            
            # Create the Site object
            current_site = Site(
                SiteId=site_dict["SiteId"],
                PlanetId=planet_id,
                PlanetIdentifier=site_dict["PlanetIdentifier"],
                PlanetName=site_dict["PlanetName"],
                PlanetFoundedEpochMs=site_dict["PlanetFoundedEpochMs"],
                InvestedPermits=site_dict["InvestedPermits"],
                MaximumPermits=site_dict["MaximumPermits"],
                UserNameSubmitted=site_dict["UserNameSubmitted"],
                Timestamp=site_dict["Timestamp"],
                Buildings=buildings_on_site,
            )
            
            # Add this site to the corresponding planet's sites dictionary
            planets_dict[planet_id].sites[current_site.SiteId] = current_site
        else:
            print(f"Warning: Site found on unknown planet {planet_id}. Skipping site data for this planet.")


    # Supplement/update planet data from Workforce
    for planet_workforce_data in fio_dynamic_data.get("workforce", []):
        planet_id = planet_workforce_data['PlanetId']
        if planet_id in planets_dict: # Only update workforce for planets that exist in all_planets_data
            # Populate workforce for the planet
            workforces_on_planet: Dict[str, int] = {}
            for wf_type_data in planet_workforce_data.get("Workforces", []):
                workforce_name = wf_type_data['WorkforceTypeName']
                workforces_on_planet[workforce_name] = wf_type_data.get('Population', 0)
            planets_dict[planet_id].workforce = workforces_on_planet
        else:
            print(f"Warning: Workforce data found for unknown planet {planet_id}. Skipping workforce data for this planet.")

    # Sites and Buildings (re-parsing for the main company.sites list)
    parsed_sites: List[Site] = []
    for site_dict in fio_dynamic_data.get("sites", []):
        buildings_on_site: Dict[str, BuildingInstance] = {}
        for bldg_data in site_dict.get("Buildings", []):
            buildings_on_site[bldg_data["SiteBuildingId"]] = BuildingInstance(
                SiteBuildingId=bldg_data["SiteBuildingId"],
                BuildingTicker=bldg_data["BuildingTicker"],
                BuildingId=bldg_data.get("BuildingId"),
                BuildingCreated=bldg_data.get("BuildingCreated"),
                BuildingName=bldg_data.get("BuildingName"),
                BuildingLastRepair=bldg_data.get("BuildingLastRepair"),
                Condition=bldg_data.get("Condition"),
                ProductionLineIds={}
            )
        parsed_sites.append(Site(
            SiteId=site_dict["SiteId"],
            PlanetId=site_dict["PlanetId"],
            PlanetIdentifier=site_dict["PlanetIdentifier"],
            PlanetName=site_dict["PlanetName"],
            PlanetFoundedEpochMs=site_dict["PlanetFoundedEpochMs"],
            InvestedPermits=site_dict["InvestedPermits"],
            MaximumPermits=site_dict["MaximumPermits"],
            UserNameSubmitted=site_dict["UserNameSubmitted"],
            Timestamp=site_dict["Timestamp"],
            Buildings=buildings_on_site,
        ))

    # Storage Items (from dynamic storage data)
    parsed_storage_items: Dict[str, StorageItem] = {}
    for storage_section in fio_dynamic_data.get("storage", []):
        for item_data in storage_section.get("StorageItems", []):
            material_ticker = item_data['MaterialTicker']
            parsed_storage_items[material_ticker] = StorageItem(
                MaterialId=item_data['MaterialId'],
                MaterialName=item_data['MaterialName'],
                MaterialTicker=item_data['MaterialTicker'],
                MaterialCategory=item_data['MaterialCategory'],
                MaterialWeight=item_data['MaterialWeight'],
                MaterialVolume=item_data['MaterialVolume'],
                MaterialAmount=item_data['MaterialAmount'],
                MaterialValue=item_data['MaterialValue'],
                MaterialValueCurrency=item_data['MaterialValueCurrency'],
                Type=item_data['Type'],
                TotalWeight=item_data['TotalWeight'],
                TotalVolume=item_data['TotalVolume']
            )

    # Market Data (from dynamic exchangeAllAvg data)
    market_data_list: List[MarketData] = []
    for market_item_data in fio_dynamic_data.get("exchangeAllAvg", []):
        market_data_list.append(MarketData(
            MaterialTicker=market_item_data['MaterialTicker'],
            PriceAverage=market_item_data['PriceAverage'],
            ExchangeCode=market_item_data['ExchangeCode'],
            MMBuy=market_item_data.get('MMBuy'),
            MMSell=market_item_data.get('MMSell'),
            AskCount=market_item_data.get('AskCount'),
            Ask=market_item_data.get('Ask'),
            Supply=market_item_data['Supply'],
            BidCount=market_item_data.get('BidCount'),
            Bid=market_item_data.get('Bid'),
            Demand=market_item_data['Demand']
        ))

    # Production Orders (from dynamic production data)
    parsed_production_orders: Dict[str, ProductionOrder] = {}
    for production_line_data in fio_dynamic_data.get("productionOverview", []):
        line_site_id = production_line_data.get("SiteId")
        line_building_name_from_overview = production_line_data.get("Type")

        actual_building_instance_id: Optional[str] = None
        site_obj: Optional[Site] = None

        if line_site_id and line_building_name_from_overview:
            for site in parsed_sites:
                if site.SiteId == line_site_id:
                    site_obj = site
                    break

            if site_obj:
                for bldg_instance_id, building_instance_obj in site_obj.Buildings.items():
                    # Match the 'Type' from production overview (which is BuildingName)
                    # with the BuildingName of the BuildingInstance object
                    if building_instance_obj.BuildingName == line_building_name_from_overview:
                        actual_building_instance_id = building_instance_obj.SiteBuildingId
                        break
                      
                if not actual_building_instance_id:
                    print(f"WARNING: Could not find a matching Building instance for production line at SiteId '{line_site_id}' with Type (BuildingName) '{line_building_name_from_overview}'. Production orders for this line might have a missing BuildingId.")
            else:
                print(f"WARNING: Site with ID '{line_site_id}' not found in the parsed_sites list for production line. Production orders for this line might have a missing BuildingId.")
        else:
            print(f"WARNING: Missing 'SiteId' or 'Type' in production line data: {production_line_data}. Cannot link to a specific building instance.")

        for order_data in production_line_data.get("Orders", []):
            order_id = order_data["ProductionLineOrderId"]
            parsed_production_orders[order_id] = ProductionOrder(
                ProductionLineOrderId=order_id,
                BuildingId=actual_building_instance_id,
                StandardRecipeName=order_data["StandardRecipeName"],
                CreatedEpochMs=order_data["CreatedEpochMs"],
                StartedEpochMs=order_data.get("StartedEpochMs"),
                CompletionEpochMs=order_data.get("CompletionEpochMs"),
                DurationMs=order_data["DurationMs"],
                LastUpdatedEpochMs=order_data["LastUpdatedEpochMs"],
                CompletedPercentage=order_data["CompletedPercentage"],
                IsHalted=order_data["IsHalted"],
                Recurring=order_data["Recurring"],
                ProductionFee=order_data.get("ProductionFee"),
                ProductionFeeCurrency=order_data.get("ProductionFeeCurrency"),
            )
    
    current_day = fio_dynamic_data.get('CurrentTick', 0)

    # ----------------------------------------------------------------------
    # 3. Create Company object (using parsed dynamic data)
    # ----------------------------------------------------------------------
    company = Company(
        id=company_id,
        name=company_name,
        cash=company_cash,
        hq=hq, # Pass the hq object here (it might have None values)
        planets=planets_dict, # Now populated from various sources
        sites=parsed_sites,
        market_data=market_data_list,
        production_orders=parsed_production_orders
    )
    
    # ----------------------------------------------------------------------
    # 4. Return the complete SimulationState
    # ----------------------------------------------------------------------
    return SimulationState(
        current_day=current_day,
        company=company,
        static_materials=static_materials,
        static_workforce_types=static_workforce_types,
        static_buildings=static_buildings,
        static_recipes=static_recipes,
        static_building_costs={},
        static_material_categories={},
        static_recipe_production_lines={},
        static_normalized_recipe_to_base_recipe_ticker_map=static_normalized_recipe_to_base_recipe_ticker_map,
        static_building_workforces=static_building_workforces,
        dynamic_market_data=market_data_list,
        all_planets_data=all_planets_data # Pass the loaded all_planets_data here
    )
