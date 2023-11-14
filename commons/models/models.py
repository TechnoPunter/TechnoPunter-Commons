from pydantic import BaseModel


class SignalReq(BaseModel):
    ltp: str
    scrip: str


class SignalResp(BaseModel):
    signal: str


class Signal(BaseModel):
    scrip: str
    low: float
    high: float
    timeStamp: str


class SignalHistory(BaseModel):
    signal: Signal
    ohlc: list
