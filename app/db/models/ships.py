from datetime import datetime
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, ConfigDict, Field

# --- Shared Config ---
model_config = ConfigDict(
    populate_by_name=True, 
    from_attributes=True
)

class Ship(BaseModel):
    """
    Represents the 'ships' table.
    """
    ship_id: str = Field(..., alias="shipid")
    user_id: str = Field(..., alias="userid")
    name: Optional[str] = Field(None)
    registration: str = Field(...)
    ship_type: Optional[str] = Field(..., alias="type")
    
    # Addressing / Location
    address_planet_id: Optional[str] = Field(None, alias="addressplanetid")
    address_station_id: Optional[str] = Field(None, alias="addressstationid")
    address_system_id: Optional[str] = Field(None, alias="addresssystemid")
    
    # Specs
    acceleration: float = Field(...)
    thrust: int = Field(...)
    volume: int = Field(...)
    mass: float = Field(...)
    operating_empty_mass: float = Field(..., alias="operatingemptymass")
    reactor_power: int = Field(..., alias="reactorpower")
    emitter_power: int = Field(..., alias="emitterpower")
    stl_fuel_flow_rate: float = Field(..., alias="stlfuelflowrate")
    ship_type: Optional[str] = Field(None, alias="shiptype")
    
    # Operational Data
    status: str = Field(...)
    condition: float = Field(...)
    commissioning_time: Optional[datetime] = Field(None, alias="commissioningtime")
    last_repair: Optional[datetime] = Field(None, alias="lastrepair")
    flight_id: Optional[str] = Field(None, alias="flightid")
    plan: Optional[Dict[str, Any]] = Field(None) # Fix the model
    
    # Stores / Operating Time
    id_ftl_fuel_store: Optional[str] = Field(None, alias="idftlfuelstore")
    id_stl_fuel_store: Optional[str] = Field(None, alias="idstlfuelstore")
    id_ship_store: Optional[str] = Field(None, alias="idshipstore")
    operating_time_ftl: int = Field(..., alias="operatingtimeftl")
    operating_time_stl: int = Field(..., alias="operatingtimestl")
    
    # Meta
    blueprint_natural_id: Optional[str] = Field(None, alias="blueprintnaturalid")
    is_owner: bool = Field(default=False)
    is_corp: bool = Field(default=False, alias="is_corp")
    company_code: Optional[str] = Field(None, alias="companycode")
    display_name: Optional[str] = Field(None, alias="displayname")
    personal_suffix: Optional[str] = Field(None, alias="personalsuffix")

    model_config = model_config

class ShipFlight(BaseModel):
    """
    Represents the 'ship_flights' table.
    """
    id: str = Field(...)
    user_id: str = Field(..., alias="userid")
    ship_id: str = Field(..., alias="shipid")
    
    # Routing
    origin_system_id: str = Field(..., alias="originsystemid")
    origin_station_id: Optional[str] = Field(None, alias="originstationid")
    origin_planet_id: Optional[str] = Field(None, alias="originplanetid")
    
    destination_system_id: str = Field(..., alias="destinationsystemid")
    destination_station_id: Optional[str] = Field(None, alias="destinationstationid")
    destination_planet_id: Optional[str] = Field(None, alias="destinationplanetid")
    
    # Status / Metrics
    departure_timestamp: datetime = Field(..., alias="departuretimestamp")
    arrival_timestamp: Optional[datetime] = Field(None, alias="arrivaltimestamp")
    current_segment_index: int = Field(..., alias="currentsegmentindex")
    aborted: bool = Field(default=False)
    damage: float = Field(default=0.0)
    
    # Consumption
    stl_distance: float = Field(..., alias="stldistance")
    stl_total_consumption: int = Field(..., alias="stltotalconsumption")
    ftl_distance: float = Field(..., alias="ftldistance")
    ftl_total_consumption: int = Field(..., alias="ftltotalconsumption")

    model_config = model_config

class ShipFlightSegment(BaseModel):
    """
    Represents the 'ship_flight_segments' table.
    """
    segment_id: int = Field(..., alias="segment_id")
    flight_id: str = Field(..., alias="flight_id")
    segment_index: int = Field(..., alias="segment_index")
    segment_type: str = Field(..., alias="segment_type")
    
    # Timing
    departure: int = Field(...)
    arrival: int = Field(...)
    duration: int = Field(...)
    
    # Locations
    origin_system_id: str = Field(..., alias="origin_system_id")
    origin_location_id: str = Field(..., alias="origin_location_id")
    origin_location_type: str = Field(..., alias="origin_location_type")
    origin_orbit_data: Dict[str, Any] = Field(..., alias="origin_orbit_data")
    
    destination_system_id: str = Field(..., alias="destination_system_id")
    destination_location_id: str = Field(..., alias="destination_location_id")
    destination_location_type: str = Field(..., alias="destination_location_type")
    destination_orbit_data: Dict[str, Any] = Field(..., alias="destination_orbit_data")
    
    # Metrics
    stl_distance: float = Field(..., alias="stl_distance")
    stl_fuel: int = Field(..., alias="stl_fuel")
    ftl_distance: float = Field(..., alias="ftl_distance")
    ftl_fuel: int = Field(..., alias="ftl_fuel")
    damage: float = Field(default=0.0)
    transfer_ellipse: Dict[str, Any] = Field(..., alias="transferellipse")

    model_config = model_config

class ShipRepairMaterial(BaseModel):
    """
    Represents the 'ship_repair_materials' table.
    """
    xata_id: str = Field(..., alias="xata_id")
    ship_id: str = Field(..., alias="shipid")
    material_id: str = Field(..., alias="materialid")
    amount: int = Field(...)
    
    xata_version: int = Field(..., alias="xata_version")
    xata_created_at: datetime = Field(..., alias="xata_createdat")
    xata_updated_at: datetime = Field(..., alias="xata_updatedat")

    model_config = model_config