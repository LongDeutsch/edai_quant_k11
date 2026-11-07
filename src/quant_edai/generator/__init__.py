"""Synthetic data generator and fault injection modules."""

__all__ = ["QuantDataGenerator", "FaultsInjector"]

def __getattr__(name: str):
    if name == "QuantDataGenerator":
        from quant_edai.generator.data_generator import QuantDataGenerator
        return QuantDataGenerator
    if name == "FaultsInjector":
        from quant_edai.generator.faults_injector import FaultsInjector
        return FaultsInjector
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
