import dataclasses
from typing import Any, Dict, List, Optional

# --- Static Game Data Models ---

@dataclasses.dataclass
class Material:
    """Represents materials, either static definitions or used in costs/recipes/site materials."""
    MaterialId: str
    MaterialName: str
    MaterialTicker: str
    CategoryName: str
    CategoryId: str
    Weight: float
    Volume: float
    UserNameSubmitted: Optional[str]
    Timestamp: Optional[str]

@dataclasses.dataclass
class WorkforceType:
    """Represents a type of workforce (e.g., Pioneers, Settlers)."""
    Name: str
    Population: int
    Required: int
    Satisfaction: float

@dataclasses.dataclass
class RecipeInput:
    """Represents a single input material for a recipe."""
    MaterialTicker: str
    Amount: float

@dataclasses.dataclass
class RecipeOutput:
    """Represents a single output material for a recipe."""
    MaterialTicker: str
    Amount: float

@dataclasses.dataclass
class Recipe:
    """Represents a crafting recipe for a building."""
    StandardRecipeName: str
    BuildingTicker: str
    RecipeName: str
    DurationMs: int
    Inputs: List[RecipeInput]
    Outputs: List[RecipeOutput]

@dataclasses.dataclass
class BuildingCostItem:
    """Represents the cost of a building in terms of materials."""
    Key: str
    Building: str
    MaterialTicker: str
    Amount: float

@dataclasses.dataclass
class BuildingWorkforceRequirement:
    """Represents the workforce required by a static building definition."""
    workforce_type: str
    amount: int

@dataclasses.dataclass
class Building:
    """Represents a static building definition."""
    ticker: str
    name: str
    area: int # Area of the building itself
    expertise: Optional[str] = None
    costs: List[BuildingCostItem] = dataclasses.field(default_factory=list)
    workforce_requirements: List[BuildingWorkforceRequirement] = dataclasses.field(default_factory=list)

@dataclasses.dataclass
class BuildingWorkforce: # This dataclass represents data from /rain/buildingworkforces FIO endpoint
    """Represents static workforce capacities provided by a building type at a certain workforce level."""
    building_ticker: str
    workforce_type_name: str
    capacity_needed: int 

@dataclasses.dataclass
class HousingWorkforce:
    WorkforceTypeTicker: str
    capacity: float

# --- Dynamic Game Data Models (FIO API Responses transformed) ---

@dataclasses.dataclass
class ProductionOrder:
    """Represents an active production order on a building."""
    ProductionLineOrderId: str
    BuildingId: Optional[str]
    StandardRecipeName: str
    CreatedEpochMs: int
    StartedEpochMs: Optional[int]
    CompletionEpochMs: Optional[int]
    DurationMs: int
    LastUpdatedEpochMs: int
    CompletedPercentage: float
    IsHalted: bool
    Recurring: bool
    ProductionFee: Optional[float]
    ProductionFeeCurrency: Optional[str]
    ProductionFeeCollectorId: Optional[str] = None
    ProductionFeeCollectorName: Optional[str] = None
    ProductionFeeCollectorCode: Optional[str] = None
    Inputs: List['RecipeInput'] = dataclasses.field(default_factory=list)
    Outputs: List['RecipeOutput'] = dataclasses.field(default_factory=list)

@dataclasses.dataclass
class BuildingInstance:
    """Represents an existing building on a planet site."""
    SiteBuildingId: str
    BuildingId: str
    BuildingCreated: int
    BuildingName: str
    BuildingTicker: str
    BuildingLastRepair: Optional[int]
    Condition: float
    ProductionLineIds: []
    production_orders: List['ProductionOrder'] = dataclasses.field(default_factory=list)
    storage_items: List['StorageItem'] = dataclasses.field(default_factory=list)

@dataclasses.dataclass
class StorageItem:
    """Represents a single material in storage (inventory or warehouse)."""
    MaterialId: str
    MaterialName: str
    MaterialTicker: str
    MaterialCategory: str
    MaterialWeight: float
    MaterialVolume: float
    MaterialAmount: float
    MaterialValue: float
    MaterialValueCurrency: str
    Type: str
    TotalWeight: float
    TotalVolume: float

@dataclasses.dataclass
class PlanetWorkforce:
    """Represents the workforce situation on a specific planet."""
    PlanetId: str
    PlanetNaturalId: str
    PlanetName: str
    SiteId: Optional[str] = None
    Workforces: Dict[str, 'WorkforceType'] = dataclasses.field(default_factory=dict)

@dataclasses.dataclass
class Planet:
    """Represents a planet in the game."""
    PlanetId: str
    NaturalId: str
    Name: str
    SystemId: str
    SystemNaturalId: str
    SystemName: str
    Type: str
    Fertility: float
    Gravity: float
    Oxygen: float
    Pressure: float
    Temperature: float
    Radius: float
    Has: Optional[List[str]] = dataclasses.field(default_factory=list)
    Nots: Optional[List[str]] = dataclasses.field(default_factory=list)
    LocalMarketId: Optional[str] = None
    Workforce: Dict[str, 'WorkforceType'] = dataclasses.field(default_factory=dict)
    Sites: Dict[str, 'Site'] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class MaterialPrice: # This might not be strictly needed if MarketData directly holds prices
    """Represents market data for a material (ask, bid, average)."""
    ticker: str
    price_average: float
    ask: Optional[float] = None
    bid: Optional[float] = None

@dataclasses.dataclass
class MarketData:
    """Represents market data for a material on a specific exchange."""
    MaterialTicker: str
    ExchangeCode: str
    MMBuy: Optional[float]
    MMSell: Optional[float]
    PriceAverage: float
    AskCount: Optional[int]
    Ask: Optional[float]
    Supply: float
    BidCount: Optional[int]
    Bid: Optional[float]
    Demand: float

@dataclasses.dataclass
class CompanyHQ:
    """Represents the company's headquarter location."""
    PlanetId: Optional[str] = None
    PlanetNaturalId: Optional[str] = None
    PlanetName: Optional[str] = None
    Tier: Optional[int] = None

@dataclasses.dataclass
class SiteBuilding:
    """Represents a building instance on a specific site (from /sites endpoint)."""
    reclaimable_materials: List[Material]
    repair_materials: List[Material]
    site_building_id: str
    building_id: str
    building_created: int
    building_name: str
    building_ticker: str
    building_last_repair: Optional[int]
    condition: float

@dataclasses.dataclass
class Site:
    """Represents a company's site on a planet, including its buildings."""
    SiteId: str
    PlanetId: str
    PlanetIdentifier: str
    PlanetName: str
    PlanetFoundedEpochMs: int
    InvestedPermits: int
    MaximumPermits: int
    UserNameSubmitted: Optional[str]
    Timestamp: str
    Buildings: Dict[str, 'BuildingInstance'] = dataclasses.field(default_factory=dict)

@dataclasses.dataclass
class Company:
    """Represents the player's company, its assets and current state."""
    id: str
    name: str
    cash: float
    hq: 'CompanyHQ'
    planets: Dict[str, 'Planet'] = dataclasses.field(default_factory=dict)
    sites: List['Site'] = dataclasses.field(default_factory=list)
    market_data: List['MarketData'] = dataclasses.field(default_factory=list)
    production_orders: Dict[str, 'ProductionOrder'] = dataclasses.field(default_factory=dict)

@dataclasses.dataclass
class SimulationState:
    """Encapsulates all data relevant to the current simulation state."""
    current_day: int
    company: 'Company'
    static_materials: Dict[str, 'Material'] = dataclasses.field(default_factory=dict)
    static_workforce_types: Dict[str, 'WorkforceType'] = dataclasses.field(default_factory=dict)
    static_buildings: Dict[str, 'Building'] = dataclasses.field(default_factory=dict)
    static_recipes: Dict[str, 'Recipe'] = dataclasses.field(default_factory=dict)
    static_building_costs: Dict[str, List['BuildingCostItem']] = dataclasses.field(default_factory=dict)
    static_material_categories: Dict[str, str] = dataclasses.field(default_factory=dict)
    static_recipe_production_lines: Dict[str, str] = dataclasses.field(default_factory=dict)
    static_normalized_recipe_to_base_recipe_ticker_map: Dict[str, str] = dataclasses.field(default_factory=dict)
    static_building_workforces: Dict[str, List['BuildingWorkforceRequirement']] = dataclasses.field(default_factory=dict)
    dynamic_market_data: List['MarketData'] = dataclasses.field(default_factory=list)
    all_planets_data: Dict[str, 'PlanetData'] = dataclasses.field(default_factory=dict)

# --- ML/AI Output Models ---

@dataclasses.dataclass
class RecommendedBuilding:
    building_ticker: str
    building_name: str
    amount_to_build: int
    estimated_cost_credits: float
    planet_id: Optional[str] = None
    site_id: Optional[str] = None

@dataclasses.dataclass
class RecommendedPlanet:
    planet_id: str
    planet_name: str
    fertility: Optional[float] = None
    resources: List['Resource'] = dataclasses.field(default_factory=list)
    total_estimated_cost_credits_on_planet: float = 0.0
    total_estimated_workforce_needed_on_planet: Dict[str, float] = dataclasses.field(default_factory=dict)
    recommended_buildings_on_planet: List[RecommendedBuilding] = dataclasses.field(default_factory=list)
    site_id: Optional[str] = None

@dataclasses.dataclass
class Recommendation:
    desired_material_ticker: str
    target_production_rate_units_per_day: float
    current_production_units_per_day: float
    production_gap_units_per_day: float
    recommended_planets: List[RecommendedPlanet] = dataclasses.field(default_factory=list)
    total_estimated_cost_credits: float = 0.0
    simulation_log: List[str] = dataclasses.field(default_factory=list)

# --- NEW DATACLASSES FOR PLANETS.JSON ---

@dataclasses.dataclass
class Resource:
    MaterialId: str
    ResourceType: str
    Factor: float

@dataclasses.dataclass
class BuildRequirement:
    MaterialName: str
    MaterialId: str
    MaterialTicker: str
    MaterialCategory: str
    MaterialAmount: int
    MaterialWeight: float
    MaterialVolume: float

@dataclasses.dataclass
class ProductionFee:
    Category: str
    WorkforceLevel: str
    FeeAmount: float
    FeeCurrency: str

@dataclasses.dataclass
class COGCProgram:
    ProgramType: Optional[str]
    StartEpochMs: int
    EndEpochMs: int

@dataclasses.dataclass
class COGCVote:
    CompanyName: str
    CompanyCode: str
    Influence: float
    VoteType: str
    VoteTimeEpochMs: int

@dataclasses.dataclass
class PlanetData:
    Resources: List[Resource]
    BuildRequirements: List[BuildRequirement]
    ProductionFees: List[ProductionFee]
    COGCPrograms: List[COGCProgram]
    COGCVotes: List[COGCVote]
    COGCUpkeep: List[Any]
    PlanetId: str
    PlanetNaturalId: str
    PlanetName: str
    Namer: Optional[str]
    NamingDataEpochMs: int
    Nameable: bool
    SystemId: str
    Gravity: float
    MagneticField: float
    Mass: float
    MassEarth: float
    OrbitSemiMajorAxis: float
    OrbitEccentricity: float
    OrbitInclination: float
    OrbitRightAscension: float
    OrbitPeriapsis: float
    OrbitIndex: int
    Pressure: float
    Radiation: float
    Radius: float
    Sunlight: float
    Surface: bool
    Temperature: float
    Fertility: float
    HasLocalMarket: bool
    HasChamberOfCommerce: bool
    HasWarehouse: bool
    HasAdministrationCenter: bool
    HasShipyard: bool
    FactionCode: Optional[str]
    FactionName: Optional[str]
    GoverningEntity: str
    CurrencyName: str
    CurrencyCode: str
    BaseLocalMarketFee: float
    LocalMarketFeeFactor: float
    WarehouseFee: float
    EstablishmentFee: float
    PopulationId: str
    COGCProgramStatus: Optional[str]
    PlanetTier: int
    UserNameSubmitted: str
    Timestamp: str
    sites: Dict[str, 'Site'] = dataclasses.field(default_factory=dict)
    workforce: Dict[str, int] = dataclasses.field(default_factory=dict)