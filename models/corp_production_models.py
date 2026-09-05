from typing import List, Optional, Dict, Any

from pydantic import BaseModel


class ProducerConsumerItem(BaseModel):
    loc: str
    player: str
    amount: float
    isAccurate: bool
    condition: float = 0.0
    batchProdActive: Optional[float] = 0.0
    batchProdQueued: Optional[float] = 0.0
    batchConsActive: Optional[float] = 0.0
    batchConsQueued: Optional[float] = 0.0


class ProductionSummaryItem(BaseModel):
    ticker: str

    productionTotal: float
    productionAccurate: float
    productionEstimated: float

    consumptionTotal: float
    consumptionAccurate: float
    consumptionEstimated: float

    net: float

    storageQty: float = 0.0
    price: float = 0.0
    marketSharePct: float = 0.0

    batchProdActive: float = 0.0
    batchProdQueued: float = 0.0
    batchConsActive: float = 0.0
    batchConsQueued: float = 0.0

    producers: List[ProducerConsumerItem]
    consumers: List[ProducerConsumerItem]
    userRecipeInputs: Optional[Dict[str, float]] = None
    userRecipesUsed: Optional[List[Dict[str, Any]]] = None


class CorpMember(BaseModel):
    companyCode: Optional[str]
    companyName: Optional[str]
    isSynchronized: bool
    lastActive: Optional[str]
    joinedDate: Optional[str]


class CorpOverviewResponse(BaseModel):
    name: str
    code: str
    memberCount: int
    headquarters: str = " - "
    productionSummary: List[ProductionSummaryItem]

    # Helper counts used by frontend widgets
    productionCount: int = 0
    consumptionCount: int = 0

    members: List[CorpMember]
    balances: List[dict] = []
