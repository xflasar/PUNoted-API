from typing import Any, Dict, List, Optional, Tuple
import math
import time
import uuid
import random

from .data_models import (
    BuildingWorkforce, HousingWorkforce, Material, WorkforceType, Building, RecipeInput, RecipeOutput, Recipe,
    ProductionOrder, BuildingInstance, StorageItem, PlanetWorkforce, Planet,
    CompanyHQ, Company, MarketData, SimulationState,
    Recommendation, RecommendedBuilding, BuildingWorkforceRequirement, Site,
    PlanetData, Resource, COGCProgram, RecommendedPlanet
)

# Helper for time conversions (ms to days, for production rates)
MS_IN_DAY = 24 * 60 * 60 * 1000

# --- CONSTANTS FOR AREA AND PERMIT MANAGEMENT ---
BASE_MAX_AREA_FIRST_PERMIT = 500.0  # Max area with 1st permit (default)
ADDITIONAL_AREA_PER_PERMIT = 250.0 # Area added by 2nd and 3rd permits
MAX_ADDITIONAL_PERMITS = 2          # Can buy up to 2 additional permits (total 3 permits)
PERMIT_COST = 50000.0               # Placeholder cost for one additional permit (in ICA)
SITE_BASE_AREA_COST = 25.0          # Area taken by the base building of a new site
# --- END CONSTANTS ---

# --- HOUSING BUILDING DEFINITIONS WITH WORKFORCE CAPACITIES ---
# This dictionary directly specifies the workforce capacity provided by each housing building type.
HOUSING_BUILDING: Dict[str, List[HousingWorkforce]] = {
    "HB1": [
        HousingWorkforce(WorkforceTypeTicker="PIONEER", capacity=100.0)
    ],
    "HB2": [
        HousingWorkforce(WorkforceTypeTicker="SETTLER", capacity=100.0)
    ],
    "HB3": [
        HousingWorkforce(WorkforceTypeTicker="TECHNICIAN", capacity=100.0)
    ],
    "HB4": [
        HousingWorkforce(WorkforceTypeTicker="ENGINEER", capacity=100.0)
    ],
    "HB5": [
        HousingWorkforce(WorkforceTypeTicker="SCIENTIST", capacity=100.0)
    ],
    "HBB": [
        HousingWorkforce(WorkforceTypeTicker="PIONEER", capacity=75.0),
        HousingWorkforce(WorkforceTypeTicker="SETTLER", capacity=75.0)
    ],
    "HBC": [
        HousingWorkforce(WorkforceTypeTicker="SETTLER", capacity=75.0),
        HousingWorkforce(WorkforceTypeTicker="TECHNICIAN", capacity=75.0)
    ],
    "HBM": [
        HousingWorkforce(WorkforceTypeTicker="TECHNICIAN", capacity=75.0),
        HousingWorkforce(WorkforceTypeTicker="ENGINEER", capacity=75.0)
    ],
    "HBL": [
        HousingWorkforce(WorkforceTypeTicker="ENGINEER", capacity=75.0),
        HousingWorkforce(WorkforceTypeTicker="SCIENTIST", capacity=75.0)
    ]
}
# --- END NEW HOUSING BUILDING DEFINITIONS ---

# Mapping from ResourceType to BuildingTicker (used to infer resource type for extractors)
RESOURCE_TYPE_TO_BUILDING_TICKER = {
    "LIQUID": "RIG",
    "MINERAL": "EXTRACTOR",
    "GASEOUS": "COLLECTOR"
}

# BUILDING_TO_COGC_CATEGORY has been removed as per user request.
# The COGC category will now be derived directly from the Building's 'Expertise' field.

def calculate_current_production(
    state: SimulationState,
    material_ticker: str,
    simulation_log: List[str],
    target_site_id: Optional[str] = None
) -> float:
    """
    Calculates the current actual production rate (units per day) of a specific
    material across all company planets, based on active production orders.
    Can be filtered to a specific site if target_site_id is provided.
    """
    total_production_units_per_day = 0.0
    
    if target_site_id:
        simulation_log.append(f"--- Calculating current production for {material_ticker} on site {target_site_id} ---")
    else:
        simulation_log.append(f"--- Calculating current production for {material_ticker} across all sites ---")

    # Iterate through all sites managed by the company
    for site_instance in state.company.sites:
        # Filter to the target site if target_site_id is provided
        if target_site_id and site_instance.SiteId != target_site_id:
            continue

        if not isinstance(site_instance, Site):
            simulation_log.append(f"  WARNING: Expected Site object but got {type(site_instance)} in company.sites.Skipping.")
            continue

        planet_name = site_instance.PlanetName
        simulation_log.append(f"  Site: {site_instance.SiteId} on Planet: {planet_name}")
        
        for building_instance in site_instance.Buildings.values():
            # The type check here should be for BuildingInstance if production orders are linked to BuildingInstance.SiteBuildingId
            if not isinstance(building_instance, BuildingInstance):
                simulation_log.append(f"    WARNING: Expected BuildingInstance object but got {type(building_instance)} in site {site_instance.SiteId}. Skipping this building.")
                continue

            building_def = state.static_buildings.get(building_instance.BuildingTicker)
            if not building_def:
                simulation_log.append(f"    WARNING: Building definition for {building_instance.BuildingTicker} not found in static data.")
                continue

            production_orders = []
            for prOrderKey, prOrderValue in state.company.production_orders.items():
                # Correctly link production order to BuildingInstance.SiteBuildingId
                if prOrderValue.BuildingId == building_instance.SiteBuildingId:
                  production_orders.append(prOrderValue)
            
            for order in production_orders:
                if not order.IsHalted and order.CompletionEpochMs is not None:
                    recipe = state.static_recipes.get(order.StandardRecipeName)
                    if not recipe:
                        simulation_log.append(f"      WARNING: Recipe not found for standard recipe name: {order.StandardRecipeName}")
                        continue

                    for output in recipe.Outputs:
                        if output.MaterialTicker == material_ticker:
                            if order.DurationMs > 0:
                                cycles_per_day = MS_IN_DAY / order.DurationMs
                                production_per_day = output.Amount * cycles_per_day
                                total_production_units_per_day += production_per_day
                                simulation_log.append(f"      Order {order.ProductionLineOrderId}: Producing {production_per_day:.2f} {material_ticker}/day from {building_def.name} on site {site_instance.SiteId}")
                            else:
                                simulation_log.append(f"      WARNING: Production order {order.ProductionLineOrderId} has 0 duration, skipping rate calculation.")
    
    if target_site_id:
        simulation_log.append(f"--- Total current production of {material_ticker} on site {target_site_id}: {total_production_units_per_day:.2f} units/day ---")
    else:
        simulation_log.append(f"--- Total current production of {material_ticker} across all sites: {total_production_units_per_day:.2f} units/day ---")
    return total_production_units_per_day

def find_best_recipe_and_building(
    static_recipes: Dict[str, Recipe],
    static_buildings: Dict[str, Building],
    desired_material_ticker: str,
    preferred_recipe_ticker: Optional[str] = None,
    preferred_building_ticker: Optional[str] = None
) -> Tuple[Optional[Recipe], Optional[str]]:
    """
    Finds the best recipe and its associated building for producing a desired material.
    Prioritizes a preferred recipe/building if provided and valid.
    """
    # 1. Prioritize existing production if specified
    if preferred_recipe_ticker and preferred_building_ticker:
        preferred_recipe = static_recipes.get(preferred_recipe_ticker)
        preferred_building_def = static_buildings.get(preferred_building_ticker)
        if preferred_recipe and preferred_building_def:
            # Check if the preferred recipe actually produces the desired material
            # and if its production building matches the preferred building ticker.
            if any(output.MaterialTicker == desired_material_ticker for output in preferred_recipe.Outputs) and \
               preferred_recipe.BuildingTicker == preferred_building_ticker:
                return preferred_recipe, preferred_building_ticker

    # 2. Fallback to searching all recipes if no preferred or preferred is not valid
    for recipe_ticker, recipe in static_recipes.items():
        if any(output.MaterialTicker == desired_material_ticker for output in recipe.Outputs):
            # Found a recipe that produces the material
            # Now find its associated building
            building_ticker = recipe.BuildingTicker
            if building_ticker and building_ticker in static_buildings:
                return recipe, building_ticker
    return None, None

def _get_material_production_category(
    material_ticker: str,
    static_recipes: Dict[str, Recipe],
    static_buildings: Dict[str, Building],
    simulation_log: List[str]
) -> Optional[str]:
    """
    Determines the broad production category for a given material ticker based on its producing building's Expertise.
    """
    producing_building_tickers = set()
    for recipe in static_recipes.values():
        if any(output.MaterialTicker == material_ticker for output in recipe.Outputs):
            if recipe.BuildingTicker:
                producing_building_tickers.add(recipe.BuildingTicker)

    if not producing_building_tickers:
        simulation_log.append(f"  Could not find any recipes producing {material_ticker}. Cannot determine its production category.")
        return None

    # Prioritize using the building's 'Expertise' field
    for b_ticker in producing_building_tickers:
        building_def = static_buildings.get(b_ticker)
        if building_def and hasattr(building_def, 'expertise') and building_def.expertise:
            category = building_def.expertise.upper() # Assuming Expertise is already in a suitable format like "AGRICULTURE"
            simulation_log.append(f"  {material_ticker} is produced by {b_ticker}. Category: {category} (from building expertise).")
            return category
    
    simulation_log.append(f"  WARNING: Could not determine COGC category for material {material_ticker} from building expertise. No specific COGC bonus will be considered.")
    return None # Return None if expertise is not found or is empty

def _get_building_expertise_category(
    building_ticker: str,
    static_buildings: Dict[str, Building],
    simulation_log: List[str]
) -> Optional[str]:
    """
    Determines the COGC category for a given building ticker directly from its Expertise field.
    """
    building_def = static_buildings.get(building_ticker)
    if building_def and hasattr(building_def, 'expertise') and building_def.expertise:
        category = building_def.expertise.upper()
        simulation_log.append(f"  Building {building_ticker} expertise category: {category}.")
        return category
    else:
        simulation_log.append(f"  WARNING: Could not determine COGC category for building {building_ticker} from its expertise. No specific COGC bonus will be considered.")
        return None


def find_best_expansion_planet(
    all_planets_data: List[PlanetData],
    static_materials: Dict[str, Material],
    static_recipes: Dict[str, Recipe],
    desired_production_type: Optional[str],
    building_ticker_for_resource_check: Optional[str],
    simulation_log: List[str]
) -> Optional[PlanetData]:
    """
    Finds the best planet for expansion based on production type (COGC category),
    COGC programs, and resource availability/fertility.
    If building_ticker_for_resource_check is provided, it will be used for precise resource checks.
    """
    simulation_log.append(f"--- Searching for best expansion planet for production type: {desired_production_type} (Building: {building_ticker_for_resource_check}) ---")
    current_epoch_ms = int(time.time() * 1000)

    candidate_planets: List[PlanetData] = [] # List of suitable planets

    for planet in all_planets_data:
        is_suitable = False
        
        # Check COGC Programs - if a planet has an active COGC program that matches the production type, it's highly preferred.
        has_active_preferred_program = False
        expected_cogc_program_type = None
        if desired_production_type: # Only try to form expected_cogc_program_type if desired_production_type is not None
            expected_cogc_program_type = f"ADVERTISING_{desired_production_type}"

        if expected_cogc_program_type: # Only iterate if we have a valid program type to look for
            for program in planet.COGCPrograms:
                if program.ProgramType == expected_cogc_program_type and \
                   program.StartEpochMs <= current_epoch_ms <= program.EndEpochMs and \
                   planet.COGCProgramStatus == "ACTIVE":
                    has_active_preferred_program = True
                    simulation_log.append(f"  Planet {planet.PlanetName} ({planet.PlanetId}) has an ACTIVE COGC program: {program.ProgramType}.")
                    break
        
        # Check Fertility for Agriculture
        if desired_production_type == "AGRICULTURE":
            if planet.Fertility > 0: # Must have positive fertility for agriculture
                is_suitable = True
                simulation_log.append(f"  Planet {planet.PlanetName} ({planet.PlanetId}) is suitable for AGRICULTURE (Fertility: {planet.Fertility:.2f}).")
            else:
                simulation_log.append(f"  Planet {planet.PlanetName} ({planet.PlanetId}) is NOT suitable for AGRICULTURE (Fertility: {planet.Fertility:.2f}).")
                continue # Skip if fertility is not positive for agriculture

        # Check Resources for Resource Extraction (now more precise)
        elif desired_production_type == "RESOURCE_EXTRACTION":
            if building_ticker_for_resource_check:
                # Find the material ticker produced by this resource extraction building
                extracted_material_ticker = None
                for recipe in static_recipes.values():
                    if recipe.BuildingTicker == building_ticker_for_resource_check:
                        if recipe.Outputs:
                            extracted_material_ticker = recipe.Outputs[0].MaterialTicker
                            break
                
                if extracted_material_ticker:
                    extracted_material_id = next((m.MaterialId for m_id, m in static_materials.items() if m.MaterialTicker == extracted_material_ticker), None)
                    if extracted_material_id:
                        resource_found = False
                        for resource in planet.Resources:
                            if resource.MaterialId == extracted_material_id and resource.Factor > 0:
                                # Also check if the resource type matches the building type (e.g., RIG for LIQUID)
                                if (building_ticker_for_resource_check == "RIG" and resource.ResourceType == "LIQUID") or \
                                   (building_ticker_for_resource_check == "EXT" and resource.ResourceType == "MINERAL") or \
                                   (building_ticker_for_resource_check == "COL" and resource.ResourceType == "GASEOUS"):
                                    is_suitable = True
                                    simulation_log.append(f"  Planet {planet.PlanetName} ({planet.PlanetId}) is suitable for {building_ticker_for_resource_check} (extracts {extracted_material_ticker}, Factor: {resource.Factor:.2f}).")
                                    resource_found = True
                                    break
                        if not resource_found:
                            simulation_log.append(f"  Planet {planet.PlanetName} ({planet.PlanetId}) does NOT have required resource {extracted_material_ticker} for {building_ticker_for_resource_check} with positive factor or correct type.")
                            continue
                    else:
                        simulation_log.append(f"  Material ID not found for extracted ticker: {extracted_material_ticker}. Cannot check specific resources.")
                        continue
                else:
                    simulation_log.append(f"  Could not determine extracted material for building {building_ticker_for_resource_check}. Falling back to general resource check.")
                    # Fallback to general check if specific material not found for building
                    if any(r.Factor > 0 for r in planet.Resources):
                        is_suitable = True
                        simulation_log.append(f"  Planet {planet.PlanetName} ({planet.PlanetId}) has some resources and is generally suitable for RESOURCE_EXTRACTION.")
                    else:
                        simulation_log.append(f"  Planet {planet.PlanetName} ({planet.PlanetId}) has no resources and is NOT suitable for RESOURCE_EXTRACTION.")
                        continue
            else: # If no specific building ticker for resource check, assume general suitability if any resource
                if any(r.Factor > 0 for r in planet.Resources):
                    is_suitable = True
                    simulation_log.append(f"  Planet {planet.PlanetName} ({planet.PlanetId}) has some resources and is generally suitable for RESOURCE_EXTRACTION.")
                else:
                    simulation_log.append(f"  Planet {planet.PlanetName} ({planet.PlanetId}) has no resources and is NOT suitable for RESOURCE_EXTRACTION.")
                    continue
        
        # For other production types (e.g., METALURGY, CHEMICAL), prioritize COGC programs and general industrial infrastructure
        elif desired_production_type in ["METALURGY", "REFINING", "CHEMICAL", "FOOD_PROCESSING", "PROCESSED_GOOD", "RECYCLING"]:
            if has_active_preferred_program:
                is_suitable = True # Highly suitable if it has the relevant COGC program
            elif planet.HasLocalMarket or planet.HasChamberOfCommerce:
                is_suitable = True # Suitable if it has general industrial infrastructure
                simulation_log.append(f"  Planet {planet.PlanetName} ({planet.PlanetId}) is generally suitable for {desired_production_type} (has local market/chamber of commerce).")
            else:
                simulation_log.append(f"  Planet {planet.PlanetName} ({planet.PlanetId}) is not ideal for {desired_production_type} (lacks COGC bonus and general industrial infrastructure).")
                continue # Skip if no clear suitability for industrial production
        else: # If desired_production_type is None or unrecognized, assume general suitability for now.
            is_suitable = True
            simulation_log.append(f"  Planet {planet.PlanetName} ({planet.PlanetId}) is considered generally suitable (production type not specific or recognized).")


        if is_suitable:
            candidate_planets.append(planet)

    if candidate_planets:
        # Prioritize planets with active preferred COGC programs first
        preferred_cogc_planets = []
        if expected_cogc_program_type: # Only filter if we have a valid program type to look for
            preferred_cogc_planets = [p for p in candidate_planets if any(prog.ProgramType == expected_cogc_program_type and prog.StartEpochMs <= current_epoch_ms <= prog.EndEpochMs and p.COGCProgramStatus == "ACTIVE" for prog in p.COGCPrograms)]
        
        if preferred_cogc_planets:
            best_planet = random.choice(preferred_cogc_planets) # Pick randomly from COGC-preferred
            simulation_log.append(f"--- Best expansion planet found (COGC-preferred): {best_planet.PlanetName} ({best_planet.PlanetId}) ---")
            return best_planet
        else:
            best_planet = random.choice(candidate_planets) # Pick randomly from general suitable
            simulation_log.append(f"--- Best expansion planet found (general suitable): {best_planet.PlanetName} ({best_planet.PlanetId}) ---")
            return best_planet
    else:
        simulation_log.append("--- No suitable expansion planet found. ---")
        return None

def _calculate_material_cost_and_source(
    material_ticker: str,
    state: SimulationState,
    simulation_log: List[str],
    recursion_depth: int = 0,
    max_recursion_depth: int = 3, # Limit recursion to prevent infinite loops
    preferred_planet_id: Optional[str] = None, # Context for local market/COGC bonuses
    is_final_material: bool = False # New flag to indicate if this is the top-level material
) -> Tuple[float, str, List[RecommendedBuilding], Optional[str]]:
    """
    Recursively calculates the most cost-effective way to acquire a material:
    either by buying it from the market or by producing it.

    Returns: (cost_per_unit, source_type, recommended_sub_buildings, source_planet_id)
    - cost_per_unit: The cost to acquire one unit of the material.
    - source_type: "BUY" or "PRODUCE".
    - recommended_sub_buildings: List of buildings needed for production chain.
    - source_planet_id: The planet where production would occur (if "PRODUCE").
    """
    if recursion_depth > max_recursion_depth:
        simulation_log.append(f"  {'  ' * recursion_depth}Recursion depth limit reached for {material_ticker}. Defaulting to market buy price.")
        # Fallback to market price if recursion limit is hit
        market_price = next((md.PriceAverage for md in state.dynamic_market_data if md.MaterialTicker == material_ticker), None)
        if market_price is not None:
            return market_price, "BUY", [], None
        else:
            return float('inf'), "UNKNOWN", [], None # Cannot acquire

    simulation_log.append(f"  {'  ' * recursion_depth}--- Analyzing cost for {material_ticker} (Depth: {recursion_depth}) ---")

    # Option 1: Buy from Market
    buy_cost_per_unit = float('inf')
    market_data = next((md for md in state.dynamic_market_data if md.MaterialTicker == material_ticker), None)
    if market_data:
        if market_data.Ask is not None:
            buy_cost_per_unit = market_data.Ask
            simulation_log.append(f"  {'  ' * recursion_depth}Market Ask price for {material_ticker}: {buy_cost_per_unit:.2f} ICA/unit.")
        elif market_data.Bid is not None:
            buy_cost_per_unit = market_data.Bid
            simulation_log.append(f"  {'  ' * recursion_depth}Market Bid price for {material_ticker}: {buy_cost_per_unit:.2f} ICA/unit.")
        elif market_data.PriceAverage is not None:
            buy_cost_per_unit = market_data.PriceAverage
            simulation_log.append(f"  {'  ' * recursion_depth}Market Average price for {material_ticker}: {buy_cost_per_unit:.2f} ICA/unit.")
        else:
            simulation_log.append(f"  {'  ' * recursion_depth}No usable market data (Ask, Bid, Average) found for {material_ticker}. Cannot buy.")
    else:
        simulation_log.append(f"  {'  ' * recursion_depth}No market data found for {material_ticker}. Cannot buy.")

    best_production_cost_per_unit = float('inf')
    best_production_recipe: Optional[Recipe] = None
    best_production_building_ticker: Optional[str] = None
    best_sub_buildings: List[RecommendedBuilding] = []
    best_production_planet_id: Optional[str] = None

    # Option 2: Produce
    possible_recipes = [r for r in state.static_recipes.values() if any(o.MaterialTicker == material_ticker for o in r.Outputs)]
    
    if not possible_recipes:
        simulation_log.append(f"  {'  ' * recursion_depth}No recipes found to produce {material_ticker}.")

    for recipe in possible_recipes:
        simulation_log.append(f"  {'  ' * recursion_depth}Evaluating recipe: {recipe.RecipeName} (Building: {recipe.BuildingTicker})")
        
        # Determine the planet for this specific production step
        planet_for_this_production: Optional[str] = preferred_planet_id
        
        # If no preferred planet is given, try to find a suitable one
        if planet_for_this_production is None:
            # Get the COGC category for the material being produced
            material_category = _get_material_production_category(
                material_ticker, state.static_recipes, state.static_buildings, simulation_log
            )
            
            if not state.all_planets_data:
                simulation_log.append(f"  {'  ' * recursion_depth}No planet data available. Cannot assign planet for {material_ticker} production. Skipping recipe {recipe.RecipeName}.")
                continue # Cannot proceed without planet data

            # Find the best planet based on material category and COGC/resources
            best_new_planet_data = find_best_expansion_planet(
                state.all_planets_data,
                state.static_materials,
                state.static_recipes, # Pass static_recipes for resource check
                material_category, # Pass the material's COGC category
                recipe.BuildingTicker, # Pass the building ticker for specific resource check
                simulation_log
            )
            if best_new_planet_data:
                planet_for_this_production = best_new_planet_data.PlanetId
                simulation_log.append(f"  {'  ' * recursion_depth}Determined new planet {best_new_planet_data.PlanetName} ({planet_for_this_production}) for {material_ticker} production.")
            else:
                simulation_log.append(f"  {'  ' * recursion_depth}Could not find a suitable planet for {material_ticker} production. Skipping recipe {recipe.RecipeName}.")
                continue # Cannot produce this material if no suitable planet found

        # Calculate input costs recursively
        total_input_cost = 0.0
        current_recipe_sub_buildings: List[RecommendedBuilding] = []
        can_produce_inputs = True

        for input_item in recipe.Inputs:
            # Pass the determined planet_for_this_production to recursive calls
            # is_final_material is False for inputs
            input_cost, input_source_type, input_sub_buildings, input_production_planet_id = _calculate_material_cost_and_source(
                input_item.MaterialTicker,
                state,
                simulation_log,
                recursion_depth + 1,
                max_recursion_depth,
                preferred_planet_id=planet_for_this_production, # Pass the determined planet as preferred for inputs
                is_final_material=False
            )
            if input_cost == float('inf'):
                simulation_log.append(f"  {'  ' * (recursion_depth + 1)}WARNING: Cannot acquire input {input_item.MaterialTicker}. Skipping recipe {recipe.RecipeName}.")
                can_produce_inputs = False
                break
            total_input_cost += input_item.Amount * input_cost
            # Ensure sub-buildings from inputs also carry the correct planet_id
            for sub_bldg in input_sub_buildings:
                if sub_bldg.planet_id is None: # If not already assigned by a deeper raw material search
                    sub_bldg.planet_id = input_production_planet_id if input_production_planet_id else planet_for_this_production
            current_recipe_sub_buildings.extend(input_sub_buildings)
        
        if not can_produce_inputs:
            continue

        # Calculate building cost (one-time cost for the production building itself)
        building_def = state.static_buildings.get(recipe.BuildingTicker)
        if not building_def:
            simulation_log.append(f"  {'  ' * recursion_depth}WARNING: Building definition for {recipe.BuildingTicker} not found. Skipping recipe.")
            continue
        
        building_construction_cost = 0.0
        building_cost_items = state.static_building_costs.get(recipe.BuildingTicker, [])
        for cost_item in building_cost_items:
            # For building costs, always use market prices (Ask, then Bid, then Average)
            cost_market_data = next((md for md in state.dynamic_market_data if md.MaterialTicker == cost_item.MaterialTicker), None)
            item_price = 0.0
            if cost_market_data:
                if cost_market_data.Ask is not None:
                    item_price = cost_market_data.Ask
                elif cost_market_data.Bid is not None:
                    item_price = cost_market_data.Bid
                elif cost_market_data.PriceAverage is not None:
                    item_price = cost_market_data.PriceAverage
            building_construction_cost += cost_item.Amount * item_price
        
        # Apply COGC bonus (simplified: assume a fixed percentage reduction in duration for relevant programs)
        effective_duration_ms = recipe.DurationMs
        if planet_for_this_production: # Use the determined planet for COGC check
            planet_data_for_cogc = next((pd for pd in state.all_planets_data if pd.PlanetId == planet_for_this_production), None)
            if planet_data_for_cogc:
                material_category = _get_material_production_category( # Use _get_material_production_category here
                    material_ticker, state.static_recipes, state.static_buildings, simulation_log
                )
                if material_category: # Only apply bonus if a category was determined
                    expected_cogc_program_type = f"ADVERTISING_{material_category}"
                    current_epoch_ms = int(time.time() * 1000) # Ensure current_epoch_ms is defined
                    for program in planet_data_for_cogc.COGCPrograms:
                        if program.ProgramType == expected_cogc_program_type and \
                           program.StartEpochMs <= current_epoch_ms <= program.EndEpochMs and \
                           planet_data_for_cogc.COGCProgramStatus == "ACTIVE":
                            # Apply a hypothetical bonus, e.g., 10% faster production
                            effective_duration_ms *= 0.90 # 10% faster
                            simulation_log.append(f"  {'  ' * recursion_depth}Applied COGC bonus for {material_category} on {planet_data_for_cogc.PlanetName}. New effective duration: {effective_duration_ms:.2f}ms.")
                            break

        # Calculate cost per unit for this recipe
        output_amount = next((o.Amount for o in recipe.Outputs if o.MaterialTicker == material_ticker), 0)
        if output_amount > 0 and effective_duration_ms > 0:
            cost_per_cycle = total_input_cost
            cost_per_unit_produced = cost_per_cycle / output_amount
            
            conceptual_building_cost_per_unit = building_construction_cost / (output_amount * (MS_IN_DAY / effective_duration_ms) * 365) # Amortize over 1 year of production
            cost_per_unit_produced += conceptual_building_cost_per_unit
            
            simulation_log.append(f"  {'  ' * recursion_depth}Production cost for {material_ticker} via {recipe.RecipeName}: {cost_per_unit_produced:.2f} ICA/unit (includes conceptual building cost).")

            if cost_per_unit_produced < best_production_cost_per_unit:
                best_production_cost_per_unit = cost_per_unit_produced
                best_production_recipe = recipe
                best_production_building_ticker = recipe.BuildingTicker
                best_sub_buildings = current_recipe_sub_buildings
                best_production_planet_id = planet_for_this_production # The planet determined for this step
        else:
            simulation_log.append(f"  {'  ' * recursion_depth}WARNING: Recipe {recipe.RecipeName} has zero output amount or duration. Skipping.")

    # Compare buy vs. produce, but force PRODUCE if it's the final material
    if is_final_material:
        if best_production_recipe:
            simulation_log.append(f"  {'  ' * recursion_depth}Decision for {material_ticker}: FORCING PRODUCE (as it's the final desired material) via {best_production_recipe.RecipeName} (Cost: {best_production_cost_per_unit:.2f})")
            # Add the main production building to the sub-buildings list
            if best_production_building_ticker:
                best_production_building_def = state.static_buildings.get(best_production_building_ticker)
                if best_production_building_def:
                    best_sub_buildings.append(
                        RecommendedBuilding(
                            building_ticker=best_production_building_ticker,
                            building_name=best_production_building_def.name,
                            amount_to_build=1, # This is for one unit of production chain, scaled later
                            estimated_cost_credits=building_construction_cost, # Cost for one building
                            planet_id=best_production_planet_id, # Assign the planet where this building is recommended
                            site_id=None # Site ID will be assigned in run_simulation
                        )
                    )
            return best_production_cost_per_unit, "PRODUCE", best_sub_buildings, best_production_planet_id
        else:
            simulation_log.append(f"  {'  ' * recursion_depth}ERROR: Cannot produce final desired material {material_ticker}. No suitable recipe found.")
            return float('inf'), "UNKNOWN", [], None # Cannot produce final material
    else: # For intermediate materials, choose based on profitability
        if buy_cost_per_unit <= best_production_cost_per_unit:
            simulation_log.append(f"  {'  ' * recursion_depth}Decision for {material_ticker}: BUY (Cost: {buy_cost_per_unit:.2f})")
            return buy_cost_per_unit, "BUY", [], [] # No sub-buildings needed if buying
        else:
            simulation_log.append(f"  {'  ' * recursion_depth}Decision for {material_ticker}: PRODUCE via {best_production_recipe.RecipeName} (Cost: {best_production_cost_per_unit:.2f})")
            # Add the main production building to the sub-buildings list
            if best_production_building_ticker:
                best_production_building_def = state.static_buildings.get(best_production_building_ticker)
                if best_production_building_def:
                    best_sub_buildings.append(
                        RecommendedBuilding(
                            building_ticker=best_production_building_ticker,
                            building_name=best_production_building_def.name,
                            amount_to_build=1, # This is for one unit of production chain, scaled later
                            estimated_cost_credits=building_construction_cost, # Cost for one building
                            planet_id=best_production_planet_id, # Assign the planet where this building is recommended
                            site_id=None # Site ID will be assigned in run_simulation
                        )
                    )
            return best_production_cost_per_unit, "PRODUCE", best_sub_buildings, best_production_planet_id


def run_simulation(
    initial_state: SimulationState,
    desired_material_ticker: str,
    target_production_rate: float,
    optimization_goal: Optional[str] = None # 'lower cost' or 'less area'
) -> Recommendation:
    """
    Runs a simulation to determine recommendations for increasing production,
    considering building area and workforce requirements, and choosing a suitable site.
    """
    simulation_log: List[str] = []
    simulation_log.append(f"Starting simulation for {desired_material_ticker} with target rate {target_production_rate:.2f} units/day.")
    if optimization_goal:
        simulation_log.append(f"Optimization goal: {optimization_goal}")

    # Calculate current overall production across all sites
    current_prod = calculate_current_production(initial_state, desired_material_ticker, simulation_log)
    
    production_gap = target_production_rate - current_prod
    simulation_log.append(f"Current overall production: {current_prod:.2f} units/day.")
    simulation_log.append(f"Production gap: {production_gap:.2f} units/day.")

    # Initialize a dictionary to hold RecommendedPlanet objects, keyed by planet_id
    planet_recommendations_map: Dict[str, RecommendedPlanet] = {}
    overall_total_estimated_cost = 0.0
    
    # This will accumulate workforce demand across all planets/sites
    overall_total_additional_workforce_demand: Dict[str, float] = {}

    # --- Determine total buildings needed if production gap exists ---
    if production_gap > 0:
        simulation_log.append(f"Production gap detected. Determining buildings required to produce {desired_material_ticker}.")
        
        # Perform production chain analysis for the desired material
        initial_context_planet_id = initial_state.company.hq.PlanetId if initial_state.company.hq else None
        
        final_cost_per_unit, source_type, chain_buildings, production_planet_id_for_top_level = _calculate_material_cost_and_source(
            desired_material_ticker,
            initial_state,
            simulation_log,
            preferred_planet_id=initial_context_planet_id,
            is_final_material=True # Set this flag for the top-level material
        )

        if source_type == "PRODUCE":
            simulation_log.append(f"  Optimal strategy for {desired_material_ticker}: PRODUCE. Estimated cost: {final_cost_per_unit:.2f} ICA/unit (per unit of {desired_material_ticker}).")
            
            # Determine the production rate per building for the *top-level* desired material
            top_level_producing_recipe, top_level_producing_building_ticker = find_best_recipe_and_building(
                initial_state.static_recipes,
                initial_state.static_buildings,
                desired_material_ticker
            )
            if top_level_producing_recipe and top_level_producing_building_ticker:
                output_material = next((output for output in top_level_producing_recipe.Outputs if output.MaterialTicker == desired_material_ticker), None)
                if output_material and top_level_producing_recipe.DurationMs > 0:
                    production_rate_per_building = (output_material.Amount / top_level_producing_recipe.DurationMs) * MS_IN_DAY
                else:
                    simulation_log.append(f"  WARNING: Top-level production recipe for {desired_material_ticker} has zero output or duration. Cannot scale production chain.")
                    production_rate_per_building = 0.0
            else:
                simulation_log.append(f"  WARNING: Could not find top-level production recipe for {desired_material_ticker}. Cannot scale production chain.")
                production_rate_per_building = 0.0


            if production_rate_per_building > 0:
                num_chain_sets_to_build = (production_gap / production_rate_per_building)
                num_chain_sets_to_build = max(1, int(num_chain_sets_to_build + 0.99))
                simulation_log.append(f"  Calculated {num_chain_sets_to_build} sets of production chain needed to meet the gap.")

                # Process chain_buildings and distribute them across planets based on area
                for sub_bldg_template in chain_buildings: # Iterate through each unique building type needed
                    building_area_per_unit = initial_state.static_buildings.get(sub_bldg_template.building_ticker).area
                    if building_area_per_unit is None:
                        simulation_log.append(f"  WARNING: Building area not defined for {sub_bldg_template.building_ticker}. Skipping area check for this building.")
                        building_area_per_unit = 0.0

                    remaining_amount_to_place = sub_bldg_template.amount_to_build * num_chain_sets_to_build

                    while remaining_amount_to_place > 0:
                        chosen_planet_id_for_building: Optional[str] = None
                        current_planet_rec: Optional[RecommendedPlanet] = None

                        # Get the COGC category for the building being placed
                        building_cogc_category = _get_building_expertise_category(
                            sub_bldg_template.building_ticker, initial_state.static_buildings, simulation_log
                        )

                        # 1. Try to use an existing recommended planet with matching COGC expertise and space
                        if building_cogc_category:
                            for planet_id_candidate, planet_rec_candidate in planet_recommendations_map.items():
                                # Check if this planet already has buildings with the same COGC expertise
                                has_matching_expertise = False
                                for existing_bldg in planet_rec_candidate.recommended_buildings_on_planet:
                                    existing_bldg_cogc = _get_building_expertise_category(existing_bldg.building_ticker, initial_state.static_buildings, simulation_log)
                                    if existing_bldg_cogc == building_cogc_category:
                                        has_matching_expertise = True
                                        break
                                
                                if has_matching_expertise:
                                    current_used_area_on_planet = sum(
                                        initial_state.static_buildings.get(b.building_ticker).area * b.amount_to_build
                                        for b in planet_rec_candidate.recommended_buildings_on_planet
                                        if initial_state.static_buildings.get(b.building_ticker) and initial_state.static_buildings.get(b.building_ticker).area is not None
                                    )
                                    max_total_area_on_planet = BASE_MAX_AREA_FIRST_PERMIT + (MAX_ADDITIONAL_PERMITS * ADDITIONAL_AREA_PER_PERMIT) - SITE_BASE_AREA_COST
                                    units_that_fit_in_remaining_area = 0
                                    if building_area_per_unit > 0:
                                        units_that_fit_in_remaining_area = math.floor((max_total_area_on_planet - current_used_area_on_planet) / building_area_per_unit)

                                    if units_that_fit_in_remaining_area > 0:
                                        chosen_planet_id_for_building = planet_id_candidate
                                        current_planet_rec = planet_rec_candidate
                                        simulation_log.append(f"  Prioritizing existing recommended planet {current_planet_rec.planet_name} ({chosen_planet_id_for_building}) for {sub_bldg_template.building_name} due to matching COGC expertise ({building_cogc_category}) and available space.")
                                        break # Found a suitable existing planet, break from this loop

                        # 2. If not placed on an existing preferred planet, find a new suitable planet
                        if chosen_planet_id_for_building is None:
                            # Get all available planets and shuffle them for random selection
                            available_planets = list(initial_state.all_planets_data)
                            random.shuffle(available_planets)

                            for planet_candidate in available_planets:
                                # Check if this planet is suitable for the building's category
                                is_planet_suitable_for_building = False
                                if building_cogc_category == "AGRICULTURE" and planet_candidate.Fertility > 0:
                                    is_planet_suitable_for_building = True
                                elif building_cogc_category == "RESOURCE_EXTRACTION":
                                    # For resource extraction buildings, check if the planet has the specific resource
                                    # This requires finding the output material of the building's recipe
                                    extracted_material_ticker = None
                                    for recipe in initial_state.static_recipes.values():
                                        if recipe.BuildingTicker == sub_bldg_template.building_ticker:
                                            if recipe.Outputs:
                                                extracted_material_ticker = recipe.Outputs[0].MaterialTicker
                                                break
                                    
                                    if extracted_material_ticker:
                                        extracted_material_id = next((m.MaterialId for m_id, m in initial_state.static_materials.items() if m.MaterialTicker == extracted_material_ticker), None)
                                        if extracted_material_id:
                                            for resource in planet_candidate.Resources:
                                                if resource.MaterialId == extracted_material_id and resource.Factor > 0:
                                                    if (sub_bldg_template.building_ticker == "RIG" and resource.ResourceType == "LIQUID") or \
                                                       (sub_bldg_template.building_ticker == "EXT" and resource.ResourceType == "MINERAL") or \
                                                       (sub_bldg_template.building_ticker == "COL" and resource.ResourceType == "GASEOUS"):
                                                        is_planet_suitable_for_building = True
                                                        break
                                    if not is_planet_suitable_for_building: # If specific resource not found or not suitable
                                        simulation_log.append(f"  Planet {planet_candidate.PlanetName} ({planet_candidate.PlanetId}) is NOT suitable for {sub_bldg_template.building_ticker} (missing specific resource or type).")
                                        continue
                                elif building_cogc_category in ["METALURGY", "REFINING", "CHEMICAL", "FOOD_PROCESSING", "PROCESSED_GOOD", "RECYCLING"]:
                                    # For processing buildings, check for general industrial suitability or COGC bonus
                                    has_active_cogc_program = False
                                    if building_cogc_category:
                                        expected_cogc_program_type = f"ADVERTISING_{building_cogc_category}"
                                        current_epoch_ms = int(time.time() * 1000)
                                        for program in planet_candidate.COGCPrograms:
                                            if program.ProgramType == expected_cogc_program_type and \
                                               program.StartEpochMs <= current_epoch_ms <= program.EndEpochMs and \
                                               planet_candidate.COGCProgramStatus == "ACTIVE":
                                                has_active_cogc_program = True
                                                break
                                    
                                    if has_active_cogc_program or planet_candidate.HasLocalMarket or planet_candidate.HasChamberOfCommerce:
                                        is_planet_suitable_for_building = True
                                    else:
                                        simulation_log.append(f"  Planet {planet_candidate.PlanetName} ({planet_candidate.PlanetId}) is not ideal for {building_cogc_category} building {sub_bldg_template.building_ticker}.")
                                else: # If no specific COGC category or unrecognized, assume generally suitable
                                    is_planet_suitable_for_building = True
                                    simulation_log.append(f"  Planet {planet_candidate.PlanetName} ({planet_candidate.PlanetId}) is considered generally suitable for building {sub_bldg_template.building_ticker} (category not specific or recognized).")

                                if not is_planet_suitable_for_building:
                                    continue # Skip this planet if not suitable for this building type

                                # Check if this planet is already in our recommendations map
                                if planet_candidate.PlanetId not in planet_recommendations_map:
                                    planet_recommendations_map[planet_candidate.PlanetId] = RecommendedPlanet(
                                        planet_id=planet_candidate.PlanetId,
                                        planet_name=planet_candidate.PlanetName,
                                        fertility=planet_candidate.Fertility,
                                        resources=planet_candidate.Resources,
                                        total_estimated_cost_credits_on_planet=0.0,
                                        total_estimated_workforce_needed_on_planet={},
                                        recommended_buildings_on_planet=[],
                                        site_id=f"NEW_SITE_{str(uuid.uuid4())[:8].upper()}"
                                    )
                                    simulation_log.append(f"  Created new RecommendedPlanet entry for {planet_candidate.PlanetName} ({planet_candidate.PlanetId}) with placeholder site ID {planet_recommendations_map[planet_candidate.PlanetId].site_id}.")
                                
                                temp_planet_rec = planet_recommendations_map[planet_candidate.PlanetId]
                                current_used_area_on_planet = sum(
                                    initial_state.static_buildings.get(b.building_ticker).area * b.amount_to_build
                                    for b in temp_planet_rec.recommended_buildings_on_planet
                                    if initial_state.static_buildings.get(b.building_ticker) and initial_state.static_buildings.get(b.building_ticker).area is not None
                                )
                                
                                max_total_area_on_planet = BASE_MAX_AREA_FIRST_PERMIT + (MAX_ADDITIONAL_PERMITS * ADDITIONAL_AREA_PER_PERMIT) - SITE_BASE_AREA_COST
                                
                                units_that_fit_in_remaining_area = 0
                                if building_area_per_unit > 0:
                                    units_that_fit_in_remaining_area = math.floor((max_total_area_on_planet - current_used_area_on_planet) / building_area_per_unit)

                                if units_that_fit_in_remaining_area > 0:
                                    chosen_planet_id_for_building = planet_candidate.PlanetId
                                    current_planet_rec = temp_planet_rec
                                    simulation_log.append(f"  Found new suitable planet {current_planet_rec.planet_name} ({chosen_planet_id_for_building}) for {sub_bldg_template.building_name}.")
                                    break # Found a planet, break from inner loop
                            
                            if chosen_planet_id_for_building is None:
                                simulation_log.append(f"  WARNING: No suitable planet found for remaining {remaining_amount_to_place} units of {sub_bldg_template.building_name}. Skipping these units.")
                                break # Cannot place remaining units, break from while loop

                        # Determine how many units to place on this chosen planet
                        units_to_place_on_this_planet = min(remaining_amount_to_place, units_that_fit_in_remaining_area)
                        
                        if units_to_place_on_this_planet == 0:
                            simulation_log.append(f"  WARNING: No space for {sub_bldg_template.building_name} on {current_planet_rec.planet_name}. Moving to next planet search.")
                            # This case means units_that_fit_in_remaining_area was 0, so we need to find a new planet.
                            # The outer while loop will continue to try and find a new planet.
                            continue # Continue to next iteration of while loop to find another planet

                        # Add the building to the chosen planet's recommendations
                        current_planet_rec.recommended_buildings_on_planet.append(
                            RecommendedBuilding(
                                building_ticker=sub_bldg_template.building_ticker,
                                building_name=sub_bldg_template.building_name,
                                amount_to_build=units_to_place_on_this_planet,
                                estimated_cost_credits=sub_bldg_template.estimated_cost_credits * units_to_place_on_this_planet,
                                planet_id=chosen_planet_id_for_building,
                                site_id=current_planet_rec.site_id
                            )
                        )
                        current_planet_rec.total_estimated_cost_credits_on_planet += sub_bldg_template.estimated_cost_credits * units_to_place_on_this_planet
                        overall_total_estimated_cost += sub_bldg_template.estimated_cost_credits * units_to_place_on_this_planet

                        # Accumulate workforce demand for the placed buildings on this planet
                        bldg_workforce_reqs = initial_state.static_building_workforces.get(sub_bldg_template.building_ticker, [])
                        for req in bldg_workforce_reqs:
                            required_amount_for_type = req.capacity_needed * units_to_place_on_this_planet
                            current_planet_rec.total_estimated_workforce_needed_on_planet[req.workforce_type_name] = (
                                current_planet_rec.total_estimated_workforce_needed_on_planet.get(req.workforce_type_name, 0.0) + required_amount_for_type
                            )
                            # Also update overall demand for housing calculation later
                            overall_total_additional_workforce_demand[req.workforce_type_name] = (
                                overall_total_additional_workforce_demand.get(req.workforce_type_name, 0.0) + required_amount_for_type
                            )
                            simulation_log.append(f"  Planet {current_planet_rec.planet_name}: Building {sub_bldg_template.building_name} ({units_to_place_on_this_planet} units) will demand {required_amount_for_type:.2f} units of {req.workforce_type_name} workforce.")
                        
                        remaining_amount_to_place -= units_to_place_on_this_planet
                        simulation_log.append(f"  Placed {units_to_place_on_this_planet} units of {sub_bldg_template.building_name} on {current_planet_rec.planet_name}. Remaining to place: {remaining_amount_to_place}.")

            else:
                simulation_log.append(f"  WARNING: Production rate per building for {desired_material_ticker} is 0 or not determined. Cannot scale production chain recommendations.")

        elif source_type == "BUY":
            simulation_log.append(f"  Optimal strategy for {desired_material_ticker}: BUY from market. Estimated cost: {final_cost_per_unit:.2f} ICA/unit.")
            overall_total_estimated_cost += final_cost_per_unit * production_gap # Cost to buy the required quantity
            simulation_log.append(f"  Total cost to buy {production_gap:.2f} units of {desired_material_ticker}: {overall_total_estimated_cost:.2f} ICA.")
            pass
        else:
            simulation_log.append(f"  Cannot determine optimal acquisition strategy for {desired_material_ticker}.")

    else:
        simulation_log.append(f"Current production of {desired_material_ticker} ({current_prod:.2f}) already meets or exceeds target ({target_production_rate:.2f}). No expansion recommended.")
        return Recommendation(
            desired_material_ticker=desired_material_ticker,
            target_production_rate_units_per_day=target_production_rate,
            current_production_units_per_day=current_prod,
            production_gap_units_per_day=production_gap,
            recommended_planets=[],
            total_estimated_cost_credits=0.0,
            simulation_log=simulation_log
        )

    # --- Determine housing needs and add to total_new_area_needed_by_recommendations ---
    simulation_log.append("--- Checking Workforce Demand and Planning Housing Buildings ---")
    for planet_id, planet_rec in planet_recommendations_map.items():
        simulation_log.append(f"  Processing housing for Planet: {planet_rec.planet_name} ({planet_id}).")
        
        for wf_type_ticker, amount_needed_on_planet in planet_rec.total_estimated_workforce_needed_on_planet.items():
            workforce_type_name = wf_type_ticker
            
            simulation_log.append(f"    Workforce Type '{workforce_type_name}' demand on {planet_rec.planet_name}: {amount_needed_on_planet:.2f} units.")

            if amount_needed_on_planet <= 0:
                simulation_log.append(f"    No demand for {workforce_type_name} on {planet_rec.planet_name}. Skipping housing for this type.")
                continue

            suitable_housing_candidates: List[Tuple[Building, float, float, float]] = [] 
            
            for hb_ticker, hb_capacities in HOUSING_BUILDING.items():
                static_hb_def = initial_state.static_buildings.get(hb_ticker)
                if not static_hb_def:
                    simulation_log.append(f"    WARNING: Static building definition not found for hardcoded housing ticker '{hb_ticker}'. Skipping.")
                    continue

                current_hb_capacity_for_type = 0.0
                for capacity_item in hb_capacities:
                    if capacity_item.WorkforceTypeTicker == wf_type_ticker and capacity_item.capacity > 0:
                        current_hb_capacity_for_type += capacity_item.capacity
                
                if current_hb_capacity_for_type > 0:
                    estimated_cost_per_housing_building = 0.0
                    housing_cost_items = initial_state.static_building_costs.get(hb_ticker, [])
                    for cost_item in housing_cost_items:
                        if initial_state.dynamic_market_data:
                            cost_market_data = next((md for md in initial_state.dynamic_market_data if md.MaterialTicker == cost_item.MaterialTicker), None)
                            item_price = 0.0
                            if cost_market_data:
                                if cost_market_data.Ask is not None:
                                    item_price = cost_market_data.Ask
                                elif cost_market_data.Bid is not None:
                                    item_price = cost_market_data.Bid
                                elif cost_market_data.PriceAverage is not None:
                                    item_price = cost_market_data.PriceAverage
                            estimated_cost_per_housing_building += cost_item.Amount * item_price
                    
                    area_per_housing_building = static_hb_def.area if static_hb_def.area is not None else 0.0

                    cost_per_unit_wf = estimated_cost_per_housing_building / current_hb_capacity_for_type if current_hb_capacity_for_type > 0 else float('inf')
                    area_per_unit_wf = area_per_housing_building / current_hb_capacity_for_type if current_hb_capacity_for_type > 0 else float('inf')

                    suitable_housing_candidates.append((static_hb_def, cost_per_unit_wf, area_per_unit_wf, current_hb_capacity_for_type))

            if suitable_housing_candidates:
                if optimization_goal == "lower cost":
                    suitable_housing_candidates.sort(key=lambda x: x[1])
                elif optimization_goal == "less area":
                    suitable_housing_candidates.sort(key=lambda x: x[2])
                else:
                    suitable_housing_candidates.sort(key=lambda x: x[1]) # Default to cost efficiency

                housing_bldg_def, _, _, capacity_per_housing_unit = suitable_housing_candidates[0]
                housing_bldg_ticker = housing_bldg_def.ticker
                
                if capacity_per_housing_unit <= 0:
                    simulation_log.append(f"    WARNING: Selected housing building '{housing_bldg_def.name}' provides 0 capacity for '{workforce_type_name}'. Skipping recommendation for this workforce type on {planet_rec.planet_name}.")
                    continue

                num_housing_buildings_needed = (amount_needed_on_planet / capacity_per_housing_unit)
                num_housing_buildings_to_build = max(1, int(num_housing_buildings_needed + 0.99))

                simulation_log.append(f"    Planning {num_housing_buildings_to_build} units of housing '{housing_bldg_def.name}' to provide {num_housing_buildings_to_build * capacity_per_housing_unit:.2f} {workforce_type_name} capacity on {planet_rec.planet_name}.")

                estimated_cost_per_housing_building = 0.0
                housing_cost_items = initial_state.static_building_costs.get(housing_bldg_ticker, [])
                for cost_item in housing_cost_items:
                    if initial_state.dynamic_market_data:
                        cost_market_data = next((md for md in initial_state.dynamic_market_data if md.MaterialTicker == cost_item.MaterialTicker), None)
                        item_price = 0.0
                        if cost_market_data:
                            if cost_market_data.Ask is not None:
                                item_price = cost_market_data.Ask
                            elif cost_market_data.Bid is not None:
                                item_price = cost_market_data.Bid
                            elif cost_market_data.PriceAverage is not None:
                                item_price = cost_market_data.PriceAverage
                        estimated_cost_per_housing_building += cost_item.Amount * item_price

                # Add housing to the current planet's recommendations
                planet_rec.total_estimated_cost_credits_on_planet += estimated_cost_per_housing_building * num_housing_buildings_to_build
                overall_total_estimated_cost += estimated_cost_per_housing_building * num_housing_buildings_to_build

                simulation_log.append(f"    Housing buildings will consume {num_housing_buildings_to_build * (housing_bldg_def.area if housing_bldg_def.area is not None else 0.0):.2f} additional area on {planet_rec.planet_name}.")

                planet_rec.recommended_buildings_on_planet.append(
                    RecommendedBuilding(
                        building_ticker=housing_bldg_ticker,
                        building_name=housing_bldg_def.name,
                        amount_to_build=num_housing_buildings_to_build,
                        estimated_cost_credits=estimated_cost_per_housing_building,
                        planet_id=planet_id,
                        site_id=planet_rec.site_id
                    )
                )
            else:
                simulation_log.append(f"    WARNING: No suitable housing building found in HOUSING_BUILDING for workforce type '{workforce_type_name}'. Cannot meet demand on {planet_rec.planet_name}.")
    # --- END HOUSING LOGIC ---

    # --- Final Area and Permit Check for all recommended new sites ---
    simulation_log.append("--- Final Area and Permit Check for all recommended new sites ---")

    # Calculate permits needed for each *new* recommended planet's site
    for planet_id, planet_rec in planet_recommendations_map.items():
        site_area_needed_on_this_planet = 0.0
        # Add the base area cost for new sites
        site_area_needed_on_this_planet += SITE_BASE_AREA_COST 
        for bldg_rec in planet_rec.recommended_buildings_on_planet:
            static_bldg_def = initial_state.static_buildings.get(bldg_rec.building_ticker)
            if static_bldg_def and static_bldg_def.area is not None:
                site_area_needed_on_this_planet += bldg_rec.amount_to_build * static_bldg_def.area

        permits_to_buy_on_this_planet = 0
        
        # Max area for a planet is 1000 (500 base + 2*250 permits).
        # We assume new sites start with 0 permits, and can buy up to MAX_ADDITIONAL_PERMITS.
        # The total area a site can support is BASE_MAX_AREA_FIRST_PERMIT + (MAX_ADDITIONAL_PERMITS * ADDITIONAL_AREA_PER_PERMIT)
        max_possible_area_on_planet = BASE_MAX_AREA_FIRST_PERMIT + (MAX_ADDITIONAL_PERMITS * ADDITIONAL_AREA_PER_PERMIT)

        if site_area_needed_on_this_planet > max_possible_area_on_planet:
            simulation_log.append(f"  WARNING: Planet {planet_rec.planet_name} ({planet_rec.planet_id}): Total area needed ({site_area_needed_on_this_planet:.2f}) exceeds maximum possible area ({max_possible_area_on_planet:.2f}) even with all permits. This site is over-capacity.")
            # Calculate permits needed to reach max capacity, even if it's not enough
            area_deficit_beyond_base = site_area_needed_on_this_planet - BASE_MAX_AREA_FIRST_PERMIT
            permits_to_buy_float = area_deficit_beyond_base / ADDITIONAL_AREA_PER_PERMIT
            permits_to_buy_on_this_planet = min(MAX_ADDITIONAL_PERMITS, max(0, int(permits_to_buy_float + 0.99)))
        else:
            # Calculate permits needed to cover the area deficit up to max_possible_area_on_planet
            area_deficit_from_base = site_area_needed_on_this_planet - BASE_MAX_AREA_FIRST_PERMIT
            if area_deficit_from_base > 0:
                permits_to_buy_float = area_deficit_from_base / ADDITIONAL_AREA_PER_PERMIT
                permits_to_buy_on_this_planet = min(MAX_ADDITIONAL_PERMITS, max(0, int(permits_to_buy_float + 0.99)))
            else:
                simulation_log.append(f"  Planet {planet_rec.planet_name} ({planet_rec.planet_id}): Sufficient area with base permit.")

        # Update total cost for this planet based on permits needed
        planet_rec.total_estimated_cost_credits_on_planet += permits_to_buy_on_this_planet * PERMIT_COST
        overall_total_estimated_cost += permits_to_buy_on_this_planet * PERMIT_COST
        simulation_log.append(f"  Planet {planet_rec.planet_name} ({planet_rec.planet_id}): Recommending {permits_to_buy_on_this_planet} permit(s). Permit Cost: {permits_to_buy_on_this_planet * PERMIT_COST:.2f} ICA.")


    # Convert the map of RecommendedPlanet objects to a list for the final recommendation
    final_recommended_planets = list(planet_recommendations_map.values())

    # Construct the final Recommendation object
    recommendation = Recommendation(
        desired_material_ticker=desired_material_ticker,
        target_production_rate_units_per_day=target_production_rate,
        current_production_units_per_day=current_prod,
        production_gap_units_per_day=production_gap,
        recommended_planets=final_recommended_planets, # Use the new list of RecommendedPlanet
        total_estimated_cost_credits=overall_total_estimated_cost, # Use the overall total cost
        simulation_log=simulation_log
    )
    
    simulation_log.append("Simulation completed.")
    return recommendation
