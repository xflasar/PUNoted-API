# data_converter.py
import json
import string
from typing import Dict, List, Any, Optional
import datetime

# ==============================================================================
# CONVERSION FUNCTIONS FOR EACH DATABASE TABLE
# Each function takes raw JSON data and transforms it into a list of dictionaries
# formatted for a specific Xata table.
# ==============================================================================

def convert_users_data_table(raw_records: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Converts raw data to match the 'users_data' table schema, handling
    None values by replacing them with a specific default value.
    """
    converted_records = []
    
    # Safely get the payload, defaulting to an empty dict if not found
    payload = raw_records.get('payload', {})
    
    # Helper function to get a value and replace None with a specified default
    def get_value_or_default(key: str, default: Any = None):
        value = payload.get(key)
        return value if value is not None else default

    # Handle subscription expiry timestamp
    subscription_expiry_data = payload.get('subscriptionExpiry')
    if subscription_expiry_data is not None and subscription_expiry_data.get('timestamp') is not None:
        subscription_expiry = datetime.datetime.fromtimestamp(subscription_expiry_data['timestamp'] / 1000)
    else:
        subscription_expiry = None

    # Handle created timestamp
    created_data = payload.get('created')
    if created_data is not None and created_data.get('timestamp') is not None:
        created = datetime.datetime.fromtimestamp(created_data['timestamp'] / 1000)
    else:
        created = None

    converted_records.append({
        'userid': get_value_or_default('id', 'null'),
        'displayname': get_value_or_default('username', 'null'),
        'companyid': get_value_or_default('companyId', 'null'),
        'subscriptionlevel': get_value_or_default('subscriptionLevel', 'null'),
        'subscriptionexpiry': subscription_expiry,
        'created': created,
        'preferredlocale': get_value_or_default('preferredLocale', 'null'),
        'highesttier': get_value_or_default('highestTier', 'null'),
        'ispayinguser': get_value_or_default('isPayingUser', 'null'),
        'ismuted': get_value_or_default('isMuted', 'null'),
        'preferredlocale': get_value_or_default('preferredLocale', 'null')
    })
    
    return converted_records

def convert_user_gifts_received_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'user_gifts_received' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'id': record.get('id'),
            'userId': record.get('userId'),
            'giftId': record.get('giftId')
        })
    return converted_records

def convert_user_gifts_sent_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'user_gifts_sent' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'id': record.get('id'),
            'userId': record.get('userId'),
            'giftId': record.get('giftId')
        })
    return converted_records

def convert_user_starting_profiles_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Converts raw data to match the 'user_starting_profiles' table schema.
    Note: 'baseMaterials', 'buildingTickers', 'workforce', and 'commodities' are JSON columns.
    """
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'name': record.get('name'),
            'ships': record.get('ships'),
            'baseMaterials': record.get('baseMaterials'),
            'buildingTickers': record.get('buildingTickers'),
            'workforce': record.get('workforce'),
            'commodities': record.get('commodities')
        })
    return converted_records

def convert_user_tokens_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'user_tokens' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'id': record.get('id'),
            'userId': record.get('userId'),
            'token': record.get('token'),
            'refreshToken': record.get('refreshToken'),
            'expiresAt': record.get('expiresAt')
        })
    return converted_records

def convert_user_data_tokens_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'user_data_tokens' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'id': record.get('id'),
            'userId': record.get('userId'),
            'token': record.get('token'),
            'permissions': record.get('permissions'),
            'status': record.get('status'),
            'createdAt': record.get('createdAt'),
            'expiresAt': record.get('expiresAt')
        })
    return converted_records

def convert_company_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'company_data' table schema."""
    converted_records = []
    company_data = {}
    representation = {}
    representationContrubutors = []
    rating_report = {}
    headquarters = {}
    headquarters_upgrade_items = []
    headquarters_efficiency_gains = []
    headquarters_efficiency_gains_next_level = []

    record = raw_records['payload']

    representation = {
        'contributednextlevelamount': record.get('representation').get('contributedNextLevel').get('amount'),
        'contributednextlevelcurrency': record.get('representation').get('contributedNextLevel').get('currency'),
        'contributedtotalamount': record.get('representation').get('contributedTotal').get('amount'),
        'contributedtotalcurrency': record.get('representation').get('contributedTotal').get('currency'),
        'currentlevel': record.get('representation').get('currentLevel'),
        'costnextlevelamount': record.get('representation').get('costNextLevel').get('amount'),
        'costnextlevelcurrency': record.get('representation').get('costNextLevel').get('currency'),
        'leftnextlevelamount': record.get('representation').get('leftNextLevel').get('amount'),
        'leftnextlevelcurrency': record.get('representation').get('leftNextLevel').get('currency')
    }
    for contributor in record.get('representation').get('contributors'):
      print("Company data representation contributors - Fail not finished!!")

    # Handle earliest contract timestamp
    earliest_contract = record.get('ratingReport').get('earliestContract')
    if earliest_contract is not None and earliest_contract.get('timestamp') is not None:
        earliest_contract = datetime.datetime.fromtimestamp(earliest_contract['timestamp'] / 1000)
    else:
        earliest_contract = None # Or None, 0, etc. based on your needs
    
    rating_report = {
        'contractcount': record.get('ratingReport').get('contractCount'),
        'earliestcontract': earliest_contract,
        'overallrating': record.get('ratingReport').get('overallRating')
    }

    # Handle subscription expiry timestamp
    next_relocation_time = record.get('headquarters').get('nextRelocationTime')
    if next_relocation_time is not None and next_relocation_time.get('timestamp') is not None:
        next_relocation_time = datetime.datetime.fromtimestamp(next_relocation_time['timestamp'] / 1000)
    else:
        next_relocation_time = None # Or None, 0, etc. based on your needs

    headquarters = {
        'addresssystemid': record.get('headquarters').get('address').get('lines')[0].get('entity').get('id'),
        'addressplanetid': record.get('headquarters').get('address').get('lines')[1].get('entity').get('id'),
        'headquarterslevel': record.get('headquarters').get('level'),
        'nextrelocationtime': next_relocation_time,
        'relocationlocked': record.get('headquarters').get('relocationLocked'),
        'basepermits': record.get('headquarters').get('basePermits'),
        'usedbasepermits': record.get('headquarters').get('usedBasePermits'),
        'additionalbasepermits': record.get('headquarters').get('additionalBasePermits'),
        'additionalproductionqueueslots': record.get('headquarters').get('additionalProductionQueueSlots')
    }

    for item in record.get('headquarters').get('inventory').get('items'):
      headquarters_upgrade_items.append({
        'materialid': item.get('material').get('id'),
        'amount': item.get('amount'),
        'amountlimit': item.get('limit')
      })

    for efficiency_gain in record.get('headquarters').get('efficiencyGains'):
        headquarters_efficiency_gains.append({
            'category': efficiency_gain.get('category'),
            'gain': efficiency_gain.get('gain')
        })

    for efficiency_gain in record.get('headquarters').get('efficiencyGainsNextLevel'):
        headquarters_efficiency_gains_next_level.append({
            'category': efficiency_gain.get('category'),
            'gain': efficiency_gain.get('gain')
        })

    converted_records= {
        'company_data': {
            'companyid': record.get('id'),
            'companyname': record.get('name'),
            'companycode': record.get('code'),
            'startinglocationsystemid': record.get('startingLocation').get('lines')[0].get('entity').get('id'),
            'startinglocationplanetid': record.get('startingLocation').get('lines')[1].get('entity').get('id'),
            'startingprofile': record.get('startingProfile'),
            'countryid': record.get('countryId')
        },
        'representation': representation,
        'representationContributors': representationContrubutors,
        'ratingReport': rating_report,
        'headquarters': headquarters,
        'headquartersUpgradeItems': headquarters_upgrade_items,
        'headquarters_efficiency_gains': headquarters_efficiency_gains,
        'headquarters_efficiency_gains_next_level': headquarters_efficiency_gains_next_level
    }
    return converted_records

def convert_world_materials_data(raw_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Converts raw material data into two separate lists of dictionaries,
    one for material_categories and one for materials.
    """
    converted_categories = []
    converted_materials = []
    
    categories_data = raw_data['payload'].get('categories', [])

    for category in categories_data:
        category_id = category.get('id')
        name = category.get('name')
        children_ids = category.get('children')
        
        # Add a record for the material_categories table
        converted_categories.append({
            'id': category_id if category_id is not None else 'null',
            'name': name if name is not None else 'null',
            #'children': json.dumps(children_ids if children_ids is not None else [])
        })

        materials = category.get('materials', [])
        for material in materials:
            material_id = material.get('id')
            name = material.get('name')
            ticker = material.get('ticker')
            weight = material.get('weight')
            volume = material.get('volume')
            resource = material.get('resource')
            
            # Add a record for the materials table
            converted_materials.append({
                'materialid': material_id if material_id is not None else 'null',
                'name': name if name is not None else 'null',
                'ticker': ticker if ticker is not None else 'null',
                'category': category_id if category_id is not None else 'null',
                'weight': weight if weight is not None else 0.0,
                'volume': volume if volume is not None else 0.0,
                'resource': resource if resource is not None else False
            })
            
    return {
        "material_categories": converted_categories,
        "materials": converted_materials
    }

def convert_headquarters_upgrade_items_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'headquarters_upgrade_items' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'id': record.get('id'),
            'headquartersId': record.get('headquartersId'),
            'materialId': record.get('materialId'),
            'amount': record.get('amount'),
            'limit': record.get('limit')
        })
    return converted_records

def convert_storages_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'storages' table schema."""
    storages = []
    for record in raw_records["payload"]["stores"]:
        storages_items = []
        storage = {
            'storageid': record.get('id'),
            'addressableid': record.get('addressableId'),
            'name': record.get('name') if record.get('name') is not None else 'null',
            'weightload': record.get('weightLoad'),
            'weightcapacity': record.get('weightCapacity'),
            'volumeload': record.get('volumeLoad'),
            'volumecapacity': record.get('volumeCapacity'),
            'fixed': record.get('fixed'),
            'tradestore': record.get('tradeStore'),
            'rank': record.get('rank'),
            'locked': record.get('locked'),
            'type': record.get('type')
        }
        for item in record.get('items', []):
            # Skip items of type 'BLOCKED' as per your original logic
            if item.get('type') == 'BLOCKED':
                continue
            
            # New check: Skip the item if the 'quantity' key is missing
            if item.get('quantity') is None:
                continue

            quantity_data = item.get('quantity')
            currency_value = quantity_data.get('value', {})

            storages_items.append({
                'storageid': record.get('id'),
                'materialid': item.get('id'),
                'quantity': quantity_data.get('amount'),
                'totalweight': item.get('weight'),
                'totalvolume': item.get('volume'),
                'currencyamount': currency_value.get('amount'),
                'currencytype': currency_value.get('currency')
            })
        storage["storage_items"] = storages_items
        storages.append(storage)
    return {
        "storages": storages
    }

def convert_warehouses_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'warehouses' table schema."""
    converted_records = []
    for record in raw_records["payload"]["storages"]:
        # Handle founded timestamp
        next_payment = record.get('nextPayment')
        if next_payment is not None and next_payment.get('timestamp') is not None:
            next_payment = datetime.datetime.fromtimestamp(next_payment['timestamp'] / 1000)
        else:
            next_payment = None 

        converted_records.append({
            'warehouseid': record.get('warehouseId'),
            'storeid': record.get('storeId'),
            'units': record.get('units'),
            'weightcapacity': record.get('weightCapacity'),
            'volumecapacity': record.get('volumeCapacity'),
            'nextpayment': next_payment,
            'feeamount': record.get('fee').get('amount'),
            'feecurrency': record.get('fee').get('currency'),
            'status': record.get('status'),
            'addresssystem': record.get('address').get('lines')[0].get('entity').get('id'),
            'addressplanet': record.get('address').get('lines')[1].get('entity').get('id')
        })
    return converted_records

def convert_storage_items_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'storage_items' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'storageId': record.get('storageId'),
            'materialId': record.get('materialId'),
            'quantity': record.get('quantity'),
            'totalWeight': record.get('totalWeight'),
            'totalVolume': record.get('totalVolume'),
            'currencyAmount': record.get('currencyAmount'),
            'currencyType': record.get('currencyType')
        })
    return converted_records

def convert_production_lines_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'production_lines' table schema."""
    converted_records = []
    for record in raw_records['payload']['productionLines']:
        converted_records.append({
            'productionlineid': record.get('id'),
            'siteid': record.get('siteId'),
            'type': record.get('type'),
            'capacity': record.get('capacity'),
            'slots': record.get('slots'),
            'efficiency': record.get('efficiency'),
            'condition': record.get('condition'),
            'orders': convert_production_line_orders_data(record.get('orders')),
            'production_templates': convert_production_line_order_production_templates_data(record.get('productionTemplates')),
            'efficiency_factors': convert_production_line_efficiency_factors(record.get('efficiencyFactors'), record.get('id')),
            'workforces': convert_production_workforces_data(record.get('workforces'), record.get('id'))

        })
    return {'siteid': raw_records['payload'].get('siteId'), 'production_lines': converted_records}

def convert_production_workforces_data(raw_records: List[Dict[str, Any]], production_line_id: string) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'production_workforces' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'productionlineid': production_line_id,
            'level': record.get('level'),
            'efficiency': record.get('efficiency')
        })
    return converted_records

def convert_production_line_orders_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'production_line_orders' table schema."""
    converted_records = []
    for record in raw_records:
        # Handle created timestamp
        created = record.get('created')
        if created is not None and created.get('timestamp') is not None:
            created = datetime.datetime.fromtimestamp(created['timestamp'] / 1000)
        else:
            created = None

        # Handle created timestamp
        started = record.get('started')
        if started is not None and started.get('timestamp') is not None:
            started = datetime.datetime.fromtimestamp(started['timestamp'] / 1000)
        else:
            started = None

        # Handle created timestamp
        completion = record.get('completion')
        if completion is not None and completion.get('timestamp') is not None:
            completion = datetime.datetime.fromtimestamp(completion['timestamp'] / 1000)
        else:
            completion = None

        # Handle created timestamp
        lastupdated = record.get('lastUpdated')
        if lastupdated is not None and lastupdated.get('timestamp') is not None:
            lastupdated = datetime.datetime.fromtimestamp(lastupdated['timestamp'] / 1000)
        else:
            lastupdated = None

        duration = record.get('duration')
        if duration is not None and duration.get('millis') is not None:
            duration = duration.get('millis')
        else:
            duration = None

        converted_records.append({
            'orderid': record.get('id'),
            'productionlineid': record.get('productionLineId'),
            'recipeid': record.get('recipeId'),
            'created': created,
            'started': started,
            'completion': completion,
            'duration': duration,
            'lastupdated': lastupdated,
            'completed': bool(record.get('completed')),
            'halted': record.get('halted'),
            'recurring': record.get('recurring'),
            'productionfeeamount': record.get('productionFee').get('amount'),
            'productionfeecurrency': record.get('productionFee').get('currency'),
            'inputs': convert_production_line_order_materials_data(record.get('inputs'), record.get('id'), 'input'),
            'outputs': convert_production_line_order_materials_data(record.get('outputs'), record.get('id'), 'output')
        })
    return converted_records

def convert_production_line_order_materials_data(raw_records: List[Dict[str, Any]], order_id: string, material_type: string) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'production_line_order_materials' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'orderid': order_id,
            'materialId': record.get('material').get('id'),
            'type': material_type,
            'amount': record.get('amount'),
            'valueAmount': record.get('value').get('amount'),
            'valueCurrency': record.get('value').get('currency')
        })
    return converted_records

def convert_production_line_order_production_templates_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    converted_records = []
    for record in raw_records:

        duration = record.get('duration')
        if duration is not None and duration.get('millis') is not None:
            duration = duration.get('millis')
        else:
            duration = None

        converted_records.append({
            'productiontemplateid': record.get('id'),
            'name': record.get('name'),
            'duration': duration,
            'efficiency': record.get('efficiency'),
            'effortfactor': record.get('effortFactor'),
            'experience': record.get('experience'),
            'input_factors': convert_templates_factors_data(record.get('inputFactors'), record.get('id'), 'input'),
            'output_factors': convert_templates_factors_data(record.get('outputFactors'), record.get('id'), 'output'),
        })
    return converted_records

def convert_templates_factors_data(raw_records: List[Dict[str, Any]], production_template_id: string, material_type: string) -> List[Dict[str, Any]]:
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'productiontemplateid': production_template_id,
            'materialid': record.get('material').get('id'),
            'factor': record.get('factor'),
            'type': material_type
        })
    return converted_records

def convert_production_line_efficiency_factors(raw_records: List[Dict[str, Any]], production_line_id: string) -> List[Dict[str, Any]]:
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'productionlineid': production_line_id,
            'expertisecategory': record.get('expertiseCategory', None),
            'type': record.get('type'),
            'effectivity': record.get('effectivity'),
            'value': record.get('value')
        })
    return converted_records

def convert_flights_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'flights' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'id': record.get('id'),
            'shipId': record.get('shipId'),
            'originSystemId': record.get('originSystemId'),
            'originPlanetId': record.get('originPlanetId'),
            'originStationId': record.get('originStationId'),
            'destinationSystemId': record.get('destinationSystemId'),
            'destinationPlanetId': record.get('destinationPlanetId'),
            'destinationStationId': record.get('destinationStationId'),
            'departureTimestamp': record.get('departureTimestamp'),
            'arrivalTimestamp': record.get('arrivalTimestamp'),
            'stlDistance': record.get('stlDistance'),
            'ftlDistance': record.get('ftlDistance'),
            'stlTotalConsumption': record.get('stlTotalConsumption'),
            'ftlTotalConsumption': record.get('ftlTotalConsumption'),
            'aborted': record.get('aborted')
        })
    return converted_records

def convert_ships_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'ships' table schema."""
    payload = raw_records['payload']
    # Determine the format of the incoming data
    if isinstance(payload, dict) and 'ships' in payload:
        # Case 1: The data is a dictionary with a 'ships' key containing a list
        records_to_process = payload['ships']
    elif isinstance(payload, dict) and 'id' in payload:
        # Case 2: The data is a single ship record dictionary.
        # Wrap it in a list to process it in the loop.
        records_to_process = [payload]
    else:
        # Case 3: The data is already a list of ships (or an empty list)
        records_to_process = payload

    # Now, the rest of your code can safely assume `records_to_process` is a list
    if not records_to_process:
        print("No records to process.")
        return
    
    converted_records = []

    for record in records_to_process:
        # Handle created timestamp
        last_repair = record.get('lastRepair')
        if last_repair is not None and last_repair.get('timestamp') is not None:
            # 🌟 FIX: Pass the datetime object directly to the query
            last_repair = datetime.datetime.fromtimestamp(last_repair['timestamp'] / 1000)
        else:
            last_repair = None # Use None instead of 'null' for a null value

        # Handle created timestamp
        commissioning_time = record.get('commissioningTime')
        if commissioning_time is not None and commissioning_time.get('timestamp') is not None:
            # 🌟 FIX: Pass the datetime object directly to the query
            commissioning_time = datetime.datetime.fromtimestamp(commissioning_time['timestamp'] / 1000)
        else:
            commissioning_time = None # Use None instead of 'null' for a null value

        repair_materials = []
        for material in record.get('repairMaterials'):
            repair_materials.append({
            'materialid': material.get('material').get('id'),
            'amount': material.get('amount'),
            'shipid': record.get('id')
            })

        converted_records.append({
            'shipid': record.get('id'),
            'idshipstore': record.get('idShipStore'),
            'idstlfuelstore': record.get('idStlFuelStore'),
            'idftlfuelstore': record.get('idFtlFuelStore'),
            'registration': record.get('registration'),
            'name': record.get('name'),
            'commissioningtime': commissioning_time,
            'blueprintnaturalid': record.get('blueprintNaturalId'),
            'addresssystemid': record.get('addressSystemId'),
            'addressplanetid': record.get('addressPlanetId'),
            'addressstationid': record.get('addressStationId'),
            'flightid': record.get('flightId'),
            'acceleration': record.get('acceleration'),
            'thrust': record.get('thrust'),
            'mass': record.get('mass'),
            'operatingemptymass': record.get('operatingEmptyMass'),
            'volume': record.get('volume'),
            'reactorpower': record.get('reactorPower'),
            'emitterpower': record.get('emitterPower'),
            'stlfuelflowrate': record.get('stlFuelFlowRate'),
            'operatingtimestl': record.get('operatingTimeStl').get('millis'),
            'operatingtimeftl': record.get('operatingTimeFtl').get('millis'),
            'condition': record.get('condition'),
            'lastrepair': last_repair,
            'status': record.get('status'),
            'repair_materials': repair_materials
        })
    return converted_records

def convert_ship_repair_materials_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'ship_repair_materials' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'shipId': record.get('shipId'),
            'materialId': record.get('materialId'),
            'amount': record.get('amount')
        })
    return converted_records

def convert_workforces_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'workforces' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'siteId': record.get('siteId'),
            'addressSystemId': record.get('addressSystemId'),
            'addressPlanetId': record.get('addressPlanetId'),
            'level': record.get('level'),
            'population': record.get('population'),
            'reserve': record.get('reserve'),
            'capacity': record.get('capacity'),
            'required': record.get('required'),
            'satisfaction': record.get('satisfaction')
        })
    return converted_records

def convert_workforce_needs_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'workforceNeeds' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'siteId': record.get('siteId'),
            'level': record.get('level'),
            'materialId': record.get('materialId'),
            'category': record.get('category'),
            'essential': record.get('essential'),
            'satisfaction': record.get('satisfaction'),
            'unitsPerInterval': record.get('unitsPerInterval'),
            'unitsPer100': record.get('unitsPer100')
        })
    return converted_records

def convert_contracts_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'contracts' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'id': record.get('id'),
            'localId': record.get('localId'),
            'timestamp': record.get('timestamp'),
            'party': record.get('party'),
            'partnerId': record.get('partnerId'),
            'partnerName': record.get('partnerName'),
            'partnerCode': record.get('partnerCode'),
            'status': record.get('status'),
            'dueDate': record.get('dueDate'),
            'preamble': record.get('preamble')
        })
    return converted_records

def convert_contract_conditions_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'contract_conditions' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'id': record.get('id'),
            'contractId': record.get('contractId'),
            'index': record.get('index'),
            'type': record.get('type'),
            'party': record.get('party'),
            'status': record.get('status'),
            'dependencies': record.get('dependencies'),
            'deadlineDuration': record.get('deadlineDuration'),
            'deadline': record.get('deadline'),
            'amountMoney': record.get('amountMoney'),
            'currencyMoney': record.get('currencyMoney'),
            'addressSystemId': record.get('addressSystemId'),
            'addressSystemName': record.get('addressSystemName'),
            'addressPlanetId': record.get('addressPlanetId'),
            'addressPlanetName': record.get('addressPlanetName'),
            'destinationSystemId': record.get('destinationSystemId'),
            'destinationSystemName': record.get('destinationSystemName'),
            'destinationStationId': record.get('destinationStationId'),
            'destinationStationName': record.get('destinationStationName'),
            'destinationPlanetId': record.get('destinationPlanetId'),
            'destinationPlanetName': record.get('destinationPlanetName'),
            'autoProvisionStoreId': record.get('autoProvisionStoreId')
        })
    return converted_records

def convert_contract_materials_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'contract_materials' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'contractConditionId': record.get('contractConditionId'),
            'materialId': record.get('materialId'),
            'amount': record.get('amount'),
            'pickedUpAmount': record.get('pickedUpAmount')
        })
    return converted_records

def convert_contract_loan_installments_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'contract_loan_installments' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'conditionId': record.get('conditionId'),
            'interestAmount': record.get('interestAmount'),
            'repaymentAmount': record.get('repaymentAmount'),
            'totalAmount': record.get('totalAmount'),
            'currency': record.get('currency')
        })
    return converted_records

def convert_sites_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'sites' table schema."""
    converted_records = []
    records = []
    if raw_records['payload'].get('sites') is not None:
        records = raw_records['payload']['sites']
    elif raw_records['payload'].get('siteId') is not None:
        records = [raw_records['payload']]

    for record in records:
        platforms = []
        building_options = []
        buildingOptionsIds = []

        for build_option in record.get('buildOptions').get('options'):
            build_option_materials = []

            for material in build_option.get('materials').get('quantities'):
                build_option_materials.append({
                    'buildingid': build_option.get('id'),
                    'materialid': material.get('material').get('id'),
                    'amount': material.get('amount')
                })

            build_option_workforce_capacities = []

            for workfoce_capacity in build_option.get('workforceCapacities'):
                build_option_workforce_capacities.append({
                    'buildingid': build_option.get('id'),
                    'workforcelevel': workfoce_capacity.get('level'),
                    'capacity': workfoce_capacity.get('capacity')
                })

            buildingOptionsIds.append(build_option.get('id'))
            
            building_options.append({
                'buildingid': build_option.get('id'),
                'name': build_option.get('name'),
                'ticker': build_option.get('ticker'),
                'type': build_option.get('type'),
                'area': build_option.get('area'),
                'expertisecategory': build_option.get('expertiseCategory'),
                'needsfertilesoil': build_option.get('needsFertileSoil'),
                'materials': build_option_materials,
                'workforcecapacities': build_option_workforce_capacities
            })

        for platform in record.get('platforms'):
            reclaimable_materials = []

            for material in platform.get('reclaimableMaterials'):
                reclaimable_materials.append({
                    'platformid': platform.get('id').replace('\x00', ''),
                    'materialid': material.get('material').get('id'),
                    'amount': material.get('amount'),
                    'materialtype': "reclaimable"
                })

            repair_materials = []

            for material in platform.get('repairMaterials'):
                repair_materials.append({
                    'platformid': platform.get('id').replace('\x00', ''),
                    'materialid': material.get('material').get('id'),
                    'amount': material.get('amount'),
                    'materialtype': "repair"
                })

                    # Handle founded timestamp
            creation_time = platform.get('creationTime')
            if creation_time is not None and creation_time.get('timestamp') is not None:
                creation_time = datetime.datetime.fromtimestamp(creation_time['timestamp'] / 1000)
            else:
                creation_time = None 

            # Handle founded timestamp
            last_repair = platform.get('lastRepair')
            if last_repair is not None and last_repair.get('timestamp') is not None:
                last_repair = datetime.datetime.fromtimestamp(last_repair['timestamp'] / 1000)
            else:
                last_repair = None 

            platforms.append({
                'platformid': platform.get('id').replace('\x00', ''),
                'siteid': platform.get('siteId'),
                'creationtime': creation_time,
                'bookvalueamount': platform.get('bookValue').get('amount'),
                'bookvaluecurrency': platform.get('bookValue').get('currency'),
                'area': platform.get('area'),
                'condition': platform.get('condition'),
                'buildingid': platform.get('module').get('reactorId'),
                'lastrepair': last_repair,
                'reclaimable_materials': reclaimable_materials,
                'repair_materials': repair_materials
            })

        # Handle founded timestamp
        founded_timestamp = record.get('founded')
        if founded_timestamp is not None and founded_timestamp.get('timestamp') is not None:
            founded_timestamp = datetime.datetime.fromtimestamp(founded_timestamp['timestamp'] / 1000)
        else:
            founded_timestamp = None 

        converted_records.append({
            'siteid': record.get('siteId'),
            'addresssystemid': record.get('address').get('lines')[0].get('entity').get('id'),
            'addressplanetid': record.get('address').get('lines')[1].get('entity').get('id'),
            'foundedtimestamp': founded_timestamp,
            'area': record.get('area'),
            'investedpermits': record.get('investedPermits'),
            'maximumpermits': record.get('maximumPermits'),
            'buildingoptions': buildingOptionsIds,
            'building_options': building_options,
            'platforms': platforms
        })
    return converted_records

def convert_site_platforms_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'site_platforms' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'id': record.get('id'),
            'siteId': record.get('siteId'),
            'buildingPlatformId': record.get('buildingPlatformId'),
            'area': record.get('area'),
            'creationTimestamp': record.get('creationTimestamp'),
            'bookValueAmount': record.get('bookValueAmount'),
            'bookValueCurrency': record.get('bookValueCurrency'),
            'condition': record.get('condition'),
            'lastRepairTimestamp': record.get('lastRepairTimestamp')
        })
    return converted_records

def convert_site_available_population_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    record = raw_records['payload']
    workforce = record.get('availableReserveWorkforce')
    converted_data = {
        'siteid': record.get('siteId'),
        'pioneer': workforce.get('PIONEER'),
        'settler': workforce.get('SETTLER'),
        'engineer': workforce.get('ENGINEER'),
        'scientist': workforce.get('SCIENTIST'),
        'technician': workforce.get('TECHNICIAN'),
    }
    return converted_data

def convert_platform_materials_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'platform_materials' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'platformId': record.get('platformId'),
            'materialType': record.get('materialType'),
            'materialId': record.get('materialId'),
            'amount': record.get('amount')
        })
    return converted_records

def convert_buildings_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'buildings' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'id': record.get('id'),
            'name': record.get('name'),
            'ticker': record.get('ticker'),
            'type': record.get('type'),
            'area': record.get('area'),
            'expertiseCategory': record.get('expertiseCategory'),
            'needsFertileSoil': record.get('needsFertileSoil'),
            'workfoceCapacitiesId': record.get('workfoceCapacitiesId'),
            'buildMaterialsId': record.get('buildMaterialsId')
        })
    return converted_records

def convert_building_build_materials_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'building_build_materials' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'buildingId': record.get('buildingId'),
            'materialId': record.get('materialId'),
            'amount': record.get('amount')
        })
    return converted_records

def convert_corporation_shareholder_holdings_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'corporation_shareholder_holdings' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'userId': record.get('userId'),
            'code': record.get('code'),
            'name': record.get('name'),
            'corporationId': record.get('corporationId'),
            'amount': record.get('amount'),
            'currency': record.get('currency')
        })
    return converted_records

def convert_sectors_data(raw_payload: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Converts a payload with a list of sectors into structured lists for database insertion.

    Args:
        raw_payload: The raw JSON data from the request body.

    Returns:
        A dictionary containing lists of records for 'sectors', 'subsectors',
        and 'subsector_vertices' tables.
    """
    
    sector_records = []
    subsector_records = []
    vertex_records = []

    for sector in raw_payload['payload'].get('sectors', []):
        external_sector_id = sector.get('id')
        
        # Prepare record for the 'sectors' table
        sector_records.append({
            'externalsectorid': external_sector_id,
            'name': sector.get('name'),
            'hexq': sector.get('hex', {}).get('q'),
            'hexr': sector.get('hex', {}).get('r'),
            'hexs': sector.get('hex', {}).get('s'),
            'size': sector.get('size')
        })

        # Prepare records for 'subsectors' and 'subsector_vertices'
        for subsector in sector.get('subsectors', []):
            external_subsector_id = subsector.get('id')

            # Add record for the 'subsectors' table
            subsector_records.append({
                'externalsubsectorid': external_subsector_id,
                'externalsectorid': external_sector_id
            })

            # Add vertex records for the 'subsector_vertices' table
            for vertex_index, vertex in enumerate(subsector.get('vertices', [])):
                vertex_records.append({
                    'externalsubsectorid': external_subsector_id,
                    'index': vertex_index,
                    'x': vertex.get('x'),
                    'y': vertex.get('y'),
                    'z': vertex.get('z')
                })

    return {
        'sectors': sector_records,
        'subsectors': subsector_records,
        'subsector_vertices': vertex_records
    }

def convert_systems_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'systems' table schema."""
    systems = []
    systems_connections = []
    for record in raw_records.get('payload', {}).get('stars', []): # Added .get() for safety
        # Ensure 'connections' key exists and is iterable
        for connection in record.get('connections', []):
            systems_connections.append({
                'systemiddestination': connection,
                'systemidorigin': record.get('systemId')
            })
        
        # Safely access nested dictionary values
        address_lines = record.get('address', {}).get('lines', [])
        natural_id = None
        if address_lines and len(address_lines) > 0:
            entity = address_lines[0].get('entity', {})
            natural_id = entity.get('naturalId')

        systems.append({
            'systemid': record.get('systemId'),
            'name': record.get('name'),
            'naturalid': natural_id,
            'type': record.get('type'),
            'positionx': record.get('position', {}).get('x'),
            'positiony': record.get('position', {}).get('y'),
            'positionz': record.get('position', {}).get('z'),
            'sectorid': record.get('sectorId'),
            'subsectorid': record.get('subSectorId')
        })
    return {
        'systems': systems,
        'systems_connections': systems_connections
    }

def convert_system_connections_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'system_connections' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'systemId': record.get('systemId'),
            'connectedSystemId': record.get('connectedSystemId')
        })
    return converted_records

def convert_planets_data(raw_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Converts raw planet data into separate lists for multiple tables.
    Returns a dictionary of lists: 'planets', 'resources', 'build_options', 'projects', 'production_fees'.
    """
    planet_list = []
    resources_list = []
    build_options_list = []
    projects_list = []
    production_fees_list = []

    # Handle the main 'planets' table
    planet_id = raw_data["payload"].get('planetId')
    data = raw_data["payload"].get('data', {})
    country = raw_data["payload"].get('country', {})

    # Handle earliest contract timestamp
    naming_date = raw_data['payload'].get('namingDate')
    if naming_date is not None and naming_date.get('timestamp') is not None:
        naming_date = datetime.datetime.fromtimestamp(naming_date['timestamp'] / 1000)
    else:
        naming_date = None # Or None, 0, etc. based on your needs
    
    planet = {
        'planetid': planet_id if planet_id is not None else 'null',
        'naturalid': raw_data["payload"].get('naturalId'),
        'name': raw_data["payload"].get('name'),
        'namer': raw_data["payload"].get('namer').get('username') if raw_data['payload'].get('namer') is not None else 'null',
        'namingdate': naming_date,
        'nameable': raw_data["payload"].get('nameable'),
        'systemid': raw_data["payload"].get("address").get("lines")[0].get("entity").get("id"),
        'sunlight': data.get('sunlight'),
        'surface': data.get('surface'),
        'temperature': data.get('temperature'),
        'plots': data.get('plots'),
        'fertility': data.get('fertility'),
        'populationid': raw_data["payload"].get('populationId'),
        'admincenterid': raw_data["payload"].get('adminCenterId'),
        'countrycode': country.get('code') if country is not None else 'null',
        'countryname': country.get('name') if country is not None else 'null',
    }

    # Handle 'resources' table
    resources = data.get('resources', [])
    for resource in resources:
        resources_list.append({
            'planetid': planet_id if planet_id is not None else 'null',
            'materialid': resource.get('materialId'),
            'type': resource.get('type'),
            'factor': resource.get('factor') if resource.get('factor') is not None else 0.0
        })

    # Handle 'build_options' table
    build_options = raw_data["payload"].get('buildOptions', {}).get('options', [])
    for option in build_options:
        build_options_list.append({
            'planetid': planet_id if planet_id is not None else 'null',
            'sitetype': option.get('siteType'),
            'billofmaterial': json.dumps(option.get('billOfMaterial', {}))
        })
        
    # Handle 'projects' table
    projects = raw_data["payload"].get('projects', [])
    for project in projects:
        projects_list.append({
            'planetid': planet_id if planet_id is not None else 'null',
            'type': project.get('type'),
            'entityid': project.get('entityId')
        })

    # Handle 'production_fees' table
    production_fees = raw_data["payload"].get('localRules', {}).get('productionFees', {}).get('fees', [])
    for fee in production_fees:
        production_fees_list.append({
            'planetid': planet_id if planet_id is not None else 'null',
            'category': fee.get('category'),
            'workforcelevel': fee.get('workforceLevel'),
            'feeamount': fee.get('fee', {}).get('amount') if fee.get('fee') is not None else 0,
            'feecurrency': fee.get('fee', {}).get('currency') if fee.get('fee') is not None else 'null'
        })

    return {
        'planets': planet,
        'resources': resources_list,
        'build_options': build_options_list,
        'projects': projects_list,
        'production_fees': production_fees_list
    }

def convert_planet_physical_data_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'planet_physical_data' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'planetId': record.get('planetId'),
            'gravity': record.get('gravity'),
            'magneticField': record.get('magneticField'),
            'mass': record.get('mass'),
            'massEarth': record.get('massEarth'),
            'pressure': record.get('pressure'),
            'radiation': record.get('radiation'),
            'radius': record.get('radius')
        })
    return converted_records

def convert_planet_orbit_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'planet_orbit' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'planetId': record.get('planetId'),
            'orbitIndex': record.get('orbitIndex'),
            'semiMajorAxis': record.get('semiMajorAxis'),
            'eccentricity': record.get('eccentricity'),
            'inclination': record.get('inclination'),
            'rightAscension': record.get('rightAscension'),
            'periapsis': record.get('periapsis')
        })
    return converted_records

def convert_planet_resources_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'planet_resources' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'planetId': record.get('planetId'),
            'materialId': record.get('materialId'),
            'type': record.get('type'),
            'factor': record.get('factor')
        })
    return converted_records

def convert_planetWorkforceFees_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'planetWorkforceFees' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'planetId': record.get('planetId'),
            'category': record.get('category'),
            'workforceLevel': record.get('workforceLevel'),
            'feeAmount': record.get('feeAmount'),
            'feeCurrency': record.get('feeCurrency')
        })
    return converted_records

def convert_planetMarketFees_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'planetMarketFees' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'planetId': record.get('planetId'),
            'productionFeeLimitFactors': record.get('productionFeeLimitFactors'),
            'localMarketFeeBase': record.get('localMarketFeeBase'),
            'localMarketFeeTimeFactor': record.get('localMarketFeeTimeFactor'),
            'warehouseFee': record.get('warehouseFee'),
            'siteEstablishmentFee': record.get('siteEstablishmentFee')
        })
    return converted_records

def convert_planetBuildOptions_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'planetBuildOptions' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'planetId': record.get('planetId'),
            'siteType': record.get('siteType'),
            'costsAmount': record.get('costsAmount'),
            'costsCurrency': record.get('costsCurrency'),
            'feeReceiver': record.get('feeReceiver')
        })
    return converted_records

def convert_planetBuildOptionMaterials_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'planetBuildOptionMaterials' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'planetId': record.get('planetId'),
            'siteType': record.get('siteType'),
            'materialId': record.get('materialId'),
            'amount': record.get('amount')
        })
    return converted_records

def convert_stations_data(raw_record: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'stations' table schema."""
    raw_record = raw_record['payload']
    # Handle subscription expiry timestamp
    commissioning_time = raw_record.get('commissioningTime')
    if commissioning_time is not None and commissioning_time.get('timestamp') is not None:
        commissioning_time = datetime.datetime.fromtimestamp(commissioning_time['timestamp'] / 1000)
    else:
        commissioning_time = None
    
    converted_record = {
        'stationid': raw_record.get('id'),
        'systemid': raw_record.get('address').get('lines')[0].get('entity').get('id'),
        'name': raw_record.get('name'),
        'naturalid': raw_record.get('naturalId'),
        'commissioningtime': commissioning_time,
        'comexid': raw_record.get('comex').get('id'),
        'warehouseid': raw_record.get('warehouseId'),
        'localmarketid': raw_record.get('localMarketId'),
        'countryid': raw_record.get('country').get('id'),
        'governingentityid': raw_record.get('governingEntity').get('id')
    }
        
    return converted_record

def convert_countries_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'countries' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'id': record.get('id'),
            'code': record.get('code'),
            'name': record.get('name'),
            'currencyName': record.get('currencyName'),
            'currencyCode': record.get('currencyCode'),
            'currencyNumericCode': record.get('currencyNumericCode'),
            'currencyDecimals': record.get('currencyDecimals')
        })
    return converted_records

def convert_commodity_exchanges_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'commodity_exchanges' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'id': record.get('id'),
            'name': record.get('name'),
            'systemId': record.get('systemId'),
            'stationId': record.get('stationId'),
            'operatorId': record.get('operatorId'),
            'currencyName': record.get('currencyName'),
            'currencyCode': record.get('currencyCode'),
            'currencyNumericCode': record.get('currencyNumericCode'),
            'currencyDecimals': record.get('currencyDecimals')
        })
    return converted_records

def convert_population_available_reserve_workforce_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'population_available_reserve_workforce' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'siteId': record.get('siteId'),
            'workforceAmountPioneer': record.get('workforceAmountPioneer'),
            'workforceAmountSettler': record.get('workforceAmountSettler'),
            'workforceAmountTechnician': record.get('workforceAmountTechnician'),
            'workforceAmountEngineer': record.get('workforceAmountEngineer'),
            'workforceAmountScientist': record.get('workforceAmountScientist')
        })
    return converted_records

def convert_comex_broker_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'comex_trade_orders' table schema."""
    converted_records = []
    record = raw_records['payload']
    buyOrders = []
    sellOrders = []
    for buy in record.get('buyingOrders'):
        buyOrders.append({
            'orderid': buy.get('id'),
            'amount': buy.get('amount'),
            'priceamount': buy.get('limit').get('amount'),
            'pricecurrency': buy.get('limit').get('currency'),
            'traderid': buy.get('trader').get('id'),
            'tradername': buy.get('trader').get('name'),
            'tradercode': buy.get('trader').get('code')
        })

    for sell in record.get('sellingOrders'):
        sellOrders.append({
            'orderid': sell.get('id'),
            'amount': sell.get('amount'),
            'priceamount': sell.get('limit').get('amount'),
            'pricecurrency': sell.get('limit').get('currency'),
            'traderid': sell.get('trader').get('id'),
            'tradername': sell.get('trader').get('name'),
            'tradercode': sell.get('trader').get('code')
        })
    # Handle earliest contract timestamp
    price_time = record.get('priceTime')
    if price_time is not None and price_time.get('timestamp') is not None:
        price_time = datetime.datetime.fromtimestamp(price_time['timestamp'] / 1000)
    else:
        price_time = None # Or None, 0, etc. based on your needs

    converted_records.append({
        'brokermaterialid': record.get('id'),
        'addresssystemid': record.get('address', {}).get('lines', [{}, {}])[0].get('entity', {}).get('id'),
        'addressstationid': record.get('address', {}).get('lines', [{}, {}])[1].get('entity', {}).get('id'),
        'exchangeid': record.get('exchange' or {}).get('id'),
        'currencyid': record.get('currency' or {}).get('code'),
        'demand': record.get('demand'),
        'supply': record.get('supply'),
        'traded': record.get('traded'),
        'ticker': record.get('ticker'),
        'askamount': (record.get('ask') or {}).get('amount'),
        'askprice': (record.get('ask') or {}).get('price', {}).get('amount'),
        'bidamount': (record.get('bid') or {}).get('amount'),
        'bidprice': (record.get('bid') or {}).get('price', {}).get('amount'),
        'high': (record.get('high') or {}).get('amount'),
        'low': (record.get('low') or {}).get('amount'),
        'materialid': record.get('material', {}).get('id'),
        'narrowpricebandhigh': (record.get('narrowPriceBand') or {}).get('high'),
        'narrowpricebandlow': (record.get('narrowPriceBand') or {}).get('low'),
        'price': (record.get('price') or {}).get('amount'),
        'priceaverage': (record.get('price') or {}).get('amount'),
        'pricetime': price_time,
        'volume': (record.get('volume') or {}).get('amount'),
        'widepricebandhigh': (record.get('widePriceBand') or {}).get('high'),
        'widepricebandlow': (record.get('widePriceBand') or {}).get('low'),
        'alltimehigh': (record.get('allTimeHigh') or {}).get('amount'),
        'alltimelow': (record.get('allTimeLow') or {}).get('amount'),
        'buy': buyOrders,
        'sell': sellOrders
    })
    return converted_records

def convert_comex_trade_orders_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'comex_trade_orders' table schema."""
    converted_records = []
    for record in raw_records['payload']['orders']:
        trades = []

        for trade in record.get('trades'):
            # Handle earliest contract timestamp
            trade_time = trade.get('time')
            if trade_time is not None and trade_time.get('timestamp') is not None:
                trade_time = datetime.datetime.fromtimestamp(trade_time['timestamp'] / 1000)
            else:
                trade_time = None # Or None, 0, etc. based on your needs

            trades.append({
                'tradeid': trade.get('id'),
                'amount': trade.get('amount'),
                'priceamount': trade.get('price').get('amount'),
                'pricecurrency': trade.get('price').get('currency'),
                'tradetime': trade_time,
                'partnerid': trade.get('partner').get('id'),
                'partnername': trade.get('partner').get('name'),
                'partnercode': trade.get('partner').get('code')
            })

        # Handle earliest contract timestamp
        created = record.get('created')
        if created is not None and created.get('timestamp') is not None:
            created = datetime.datetime.fromtimestamp(created['timestamp'] / 1000)
        else:
            created = None # Or None, 0, etc. based on your needs

        converted_records.append({
            'orderid': record.get('id'),
            'exchangeid': record.get('exchange').get('id'),
            'brokerid': record.get('brokerId'),
            'type': record.get('type'),
            'materialid': record.get('material').get('id'),
            'amount': record.get('amount'),
            'initialamount': record.get('initialAmount'),
            'limitamount': record.get('limit').get('amount'),
            'limitcurrency': record.get('limit').get('currency'),
            'status': record.get('status'),
            'created': created,
            'trades': trades
        })
    return converted_records

def convert_comex_trade_orders_trades_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'comex_trade_orders_trades' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'id': record.get('id'),
            'orderId': record.get('orderId'),
            'amount': record.get('amount'),
            'priceAmount': record.get('priceAmount'),
            'priceCurrency': record.get('priceCurrency'),
            'timeTimestamp': record.get('timeTimestamp'),
            'partnerId': record.get('partnerId')
        })
    return converted_records

def convert_shipyard_projects_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'shipyard_projects' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'id': record.get('id'),
            'creationTimestamp': record.get('creationTimestamp'),
            'startTimestamp': record.get('startTimestamp'),
            'endTimestamp': record.get('endTimestamp'),
            'blueprintNaturalId': record.get('blueprintNaturalId'),
            'originBlueprintNaturalId': record.get('originBlueprintNaturalId'),
            'shipyardId': record.get('shipyardId'),
            'status': record.get('status'),
            'canStart': record.get('canStart'),
            'shipId': record.get('shipId')
        })
    return converted_records

def convert_shipyard_project_materials_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'shipyard_project_materials' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'projectId': record.get('projectId'),
            'materialId': record.get('materialId'),
            'amount': record.get('amount'),
            'limit': record.get('limit')
        })
    return converted_records

def convert_shipyards_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'shipyards' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'id': record.get('id'),
            'systemId': record.get('systemId'),
            'planetId': record.get('planetId'),
            'currencyId': record.get('currencyId'),
            'operatorType': record.get('operatorType'),
            'createdProjectsTotal': record.get('createdProjectsTotal'),
            'activeProjectsTotal': record.get('activeProjectsTotal'),
            'finishedProjectsTotal': record.get('finishedProjectsTotal'),
            'finishedProjectsWeek': record.get('finishedProjectsWeek'),
            'finishedProjectsMonth': record.get('finishedProjectsMonth'),
            'finishedProjectsSemiannually': record.get('finishedProjectsSemiannually')
        })
    return converted_records

def convert_ship_blueprints_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'ship_blueprints' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'id': record.get('id'),
            'naturalId': record.get('naturalId'),
            'createdTimestamp': record.get('createdTimestamp'),
            'name': record.get('name'),
            'buildTime': record.get('buildTime'),
            'status': record.get('status')
        })
    return converted_records

def convert_ship_blueprint_bill_of_materials_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'ship_blueprint_bill_of_materials' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'blueprintId': record.get('blueprintId'),
            'materialId': record.get('materialId'),
            'amount': record.get('amount')
        })
    return converted_records

def convert_ship_blueprint_components_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'ship_blueprint_components' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'id': record.get('id'),
            'blueprintId': record.get('blueprintId'),
            'type': record.get('type'),
            'cardinality': record.get('cardinality'),
            'option': record.get('option'),
            'optionMaterialId': record.get('optionMaterialId'),
            'amount': record.get('amount')
        })
    return converted_records

def convert_blueprint_components_modifiers_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'blueprint_components_modifiers' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'componentId': record.get('componentId'),
            'type': record.get('type'),
            'value': record.get('value')
        })
    return converted_records

def convert_blueprint_performance_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'blueprint_performance' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'blueprintId': record.get('blueprintId'),
            'acceleration': record.get('acceleration'),
            'accelerationMax': record.get('accelerationMax'),
            'emitterChargeTime': record.get('emitterChargeTime'),
            'fltFuelCapacity': record.get('fltFuelCapacity'),
            'fltMaxSpeed': record.get('fltMaxSpeed'),
            'maxGFactor': record.get('maxGFactor'),
            'maxOverchargeTime': record.get('maxOverchargeTime'),
            'minReactorUsage': record.get('minReactorUsage'),
            'operatingEmptyMass': record.get('operatingEmptyMass'),
            'stlFuelCapacity': record.get('stlFuelCapacity'),
            'storeCapacityVolume': record.get('storeCapacityVolume'),
            'storeCapacityMass': record.get('storeCapacityMass'),
            'totalVolume': record.get('totalVolume')
        })
    return converted_records

def convert_ship_blueprints_component_options_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'ship_blueprints_component_options' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'type': record.get('type'),
            'option': record.get('option'),
            'materialName': record.get('materialName')
        })
    return converted_records

def convert_ship_blueprints_component_types_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'ship_blueprints_component_types' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'type': record.get('type'),
            'cardinality': record.get('cardinality'),
            'selectable': record.get('selectable')
        })
    return converted_records

def convert_site_experts_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'site_experts' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'siteId': record.get('siteId'),
            'category': record.get('category'),
            'current': record.get('current'),
            'limit': record.get('limit'),
            'available': record.get('available'),
            'efficiencyGain': record.get('efficiencyGain'),
            'progress': record.get('progress')
        })
    return converted_records

def convert_cocg_programs_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'cocg_programs' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'id': record.get('id'),
            'category': record.get('category')
        })
    return converted_records

def convert_corporations_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'corporations' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'id': record.get('id'),
            'name': record.get('name'),
            'code': record.get('code'),
            'countryId': record.get('countryId'),
            'currencyCode': record.get('currencyCode'),
            'foundedTimestamp': record.get('foundedTimestamp'),
            'totalShares': record.get('totalShares')
        })
    return converted_records

def convert_corporation_shareholders_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'corporation_shareholders' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'corporationId': record.get('corporationId'),
            'userId': record.get('userId'),
            'relativeShare': record.get('relativeShare'),
            'shares': record.get('shares')
        })
    return converted_records

def convert_corporation_projects_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'corporation_projects' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'id': record.get('id'),
            'naturalId': record.get('naturalId'),
            'type': record.get('type'),
            'corporationId': record.get('corporationId'),
            'systemId': record.get('systemId'),
            'planetId': record.get('planetId'),
            'completionDate': record.get('completionDate')
        })
    return converted_records

def convert_corporation_project_bill_of_materials_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'corporation_project_bill_of_materials' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'projectId': record.get('projectId'),
            'materialId': record.get('materialId'),
            'amount': record.get('amount'),
            'currentAmount': record.get('currentAmount')
        })
    return converted_records

def convert_corporation_project_bill_contributions_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'corporation_project_bill_contributions' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'projectId': record.get('projectId'),
            'userId': record.get('userId'),
            'materialId': record.get('materialId'),
            'amount': record.get('amount'),
            'timestamp': record.get('timestamp')
        })
    return converted_records

def convert_currencies_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'currencies' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({
            'numericCode': record.get('numericCode'),
            'code': record.get('code'),
            'name': record.get('name'),
            'decimals': record.get('decimals')
        })
    return converted_records

def convert_user_currency_accounts_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'user_currency_accounts' table schema."""
    converted_records = []
    for record in raw_records['payload'].get('currencyAccounts'):
        converted_records.append({
            'category': record.get('category'),
            'type': record.get('type'),
            'number': record.get('number'),
            'bookbalanceamount': record.get('bookBalance').get('amount'),
            'bookbalancecurrencycode': record.get('bookBalance').get('currency'),
            'balanceamount': record.get('currencyBalance').get('amount'),
            'balancecurrencycode': record.get('currencyBalance').get('currency')
        })
    return converted_records

def convert_accounting_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'user_currency_accounts' table schema."""
    converted_records = []
    for record in raw_records['payload'].get('items'):
        if record.get('accountCategory') == 'LIQUID_ASSETS':
            converted_records.append({
                'category': record.get('accountCategory'),
                'type': record.get('accountType'),
                'number': record.get('account'),
                'bookbalanceamount': record.get('bookBalance').get('amount'),
                'balanceamount': record.get('balance').get('amount')
            })
    return converted_records

# ==============================================================================
# A central mapping for easy access to conversion functions.
# This makes it easy for a main script to find the right function.
# ==============================================================================

CONVERSION_FUNCTIONS = {
    'users_data': convert_users_data_table,
    'user_gifts_received': convert_user_gifts_received_data,
    'user_gifts_sent': convert_user_gifts_sent_data,
    'user_starting_profiles': convert_user_starting_profiles_data,
    'user_tokens': convert_user_tokens_data,
    'user_data_tokens': convert_user_data_tokens_data,
    'company_data': convert_company_data,
    'world_material_categories': convert_world_materials_data,
    'headquarters_upgrade_items': convert_headquarters_upgrade_items_data,
    'storages': convert_storages_data,
    'warehouses': convert_warehouses_data,
    'storage_items': convert_storage_items_data,
    'production_lines': convert_production_lines_data,
    'production_workforces': convert_production_workforces_data,
    'production_line_orders': convert_production_line_orders_data,
    'production_line_order_materials': convert_production_line_order_materials_data,
    'flights': convert_flights_data,
    'ships': convert_ships_data,
    'ship_repair_materials': convert_ship_repair_materials_data,
    'workforces': convert_workforces_data,
    'workforceNeeds': convert_workforce_needs_data,
    'contracts': convert_contracts_data,
    'contract_conditions': convert_contract_conditions_data,
    'contract_materials': convert_contract_materials_data,
    'contract_loan_installments': convert_contract_loan_installments_data,
    'sites': convert_sites_data,
    'site_platforms': convert_site_platforms_data,
    'platform_materials': convert_platform_materials_data,
    'buildings': convert_buildings_data,
    'building_build_materials': convert_building_build_materials_data,
    'corporation_shareholder_holdings': convert_corporation_shareholder_holdings_data,
    'sectors': convert_sectors_data,
    'systems': convert_systems_data,
    'system_connections': convert_system_connections_data,
    'planets': convert_planets_data,
    'planet_physical_data': convert_planet_physical_data_data,
    'planet_orbit': convert_planet_orbit_data,
    'planet_resources': convert_planet_resources_data,
    'planetWorkforceFees': convert_planetWorkforceFees_data,
    'planetMarketFees': convert_planetMarketFees_data,
    'planetBuildOptions': convert_planetBuildOptions_data,
    'planetBuildOptionMaterials': convert_planetBuildOptionMaterials_data,
    'stations': convert_stations_data,
    'countries': convert_countries_data,
    'commodity_exchanges': convert_commodity_exchanges_data,
    'population_available_reserve_workforce': convert_population_available_reserve_workforce_data,
    'comex_trade_orders': convert_comex_trade_orders_data,
    'comex_trade_orders_trades': convert_comex_trade_orders_trades_data,
    'shipyard_projects': convert_shipyard_projects_data,
    'shipyard_project_materials': convert_shipyard_project_materials_data,
    'shipyards': convert_shipyards_data,
    'ship_blueprints': convert_ship_blueprints_data,
    'ship_blueprint_bill_of_materials': convert_ship_blueprint_bill_of_materials_data,
    'ship_blueprint_components': convert_ship_blueprint_components_data,
    'blueprint_components_modifiers': convert_blueprint_components_modifiers_data,
    'blueprint_performance': convert_blueprint_performance_data,
    'ship_blueprints_component_options': convert_ship_blueprints_component_options_data,
    'ship_blueprints_component_types': convert_ship_blueprints_component_types_data,
    'site_experts': convert_site_experts_data,
    'cocg_programs': convert_cocg_programs_data,
    'corporations': convert_corporations_data,
    'corporation_shareholders': convert_corporation_shareholders_data,
    'corporation_projects': convert_corporation_projects_data,
    'corporation_project_bill_of_materials': convert_corporation_project_bill_of_materials_data,
    'corporation_project_bill_contributions': convert_corporation_project_bill_contributions_data,
    'currencies': convert_currencies_data,
    'user_currency_accounts': convert_user_currency_accounts_data,
}