from typing import List, Optional

from pydantic import BaseModel


class ProducerConsumerItem(BaseModel):
    loc: str
    player: str
    amount: float
    isAccurate: bool
    condition: float = 0.0


class ProductionSummaryItem(BaseModel):
    ticker: str

    productionTotal: float
    productionAccurate: float
    productionEstimated: float

    consumptionTotal: float
    consumptionAccurate: float
    consumptionEstimated: float

    net: float

    producers: List[ProducerConsumerItem]
    consumers: List[ProducerConsumerItem]


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
