from enum import Enum


class SourceSystem(str, Enum):
    CRYPTO = "crypto"
    CARDS = "cards"


class MatchCategory(str, Enum):
    MATCHED = "Matched"
    TIMING_DIFFERENCE = "TimingDifference"
    VARIANCE = "Variance"

