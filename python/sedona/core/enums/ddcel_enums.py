from enum import Enum

import attr

from sedona.core.jvm.abstract import JvmObject
from sedona.utils.decorators import require


class RemMethod(Enum):
    DDCEL_IC = "DDCEL_IC"
    DDCEL_RH = "DDCEL_RH"

    @classmethod
    def from_string(cls, method: str):
        try:
            rem_method = getattr(cls, method)
        except AttributeError:
            raise AttributeError(f"Can not found {method}")
        return rem_method

@attr.s
class RemMethodJvm(JvmObject):
    rem_method = attr.ib(type=RemMethod)

    def _create_jvm_instance(self):
        return self.jvm_rem_method(self.rem_method.value) if self.rem_method.value is not None else None

    @property
    @require(["RemMethod"])
    def jvm_rem_method(self):
        return self.jvm.RemMethod.getRemMethod


class RepartitioningScheme(Enum):
    ONE_LU = "ONE_LU"
    TWO_LU = "TWO_LU"
    M1LU = "M1LU"
    MU = "MU"

    @classmethod
    def from_string(cls, scheme: str):
        try:
            repartition_scheme = getattr(cls, scheme)
        except AttributeError:
            raise AttributeError(f"Can not found {scheme}")
        return repartition_scheme


@attr.s
class RepartitioningSchemeJvm(JvmObject):
    repartition_scheme = attr.ib(type=RepartitioningScheme)

    def _create_jvm_instance(self):
        return self.jvm_repartition_scheme(self.repartition_scheme.value) if self.repartition_scheme.value is not None else None

    @property
    @require(["RepartitioningScheme"])
    def jvm_repartition_scheme(self):
        return self.jvm.RepartitioningScheme.getRepartitioningScheme
