"""Existing orbital interface (stubs only, no logic).

Design: generic value objects resolved through the
``aiida.tools.data.orbitals`` entry-point group, stored in generic
containers. One container type holds any orbital flavor.
"""

from __future__ import annotations


def OrbitalFactory(entry_point_name: str, load: bool = True): ...


class Orbital:
    def __init__(self, **kwargs) -> None: ...
    def set_orbital_dict(self, init_dict: dict) -> None: ...
    def get_orbital_dict(self) -> dict: ...


class RealhydrogenOrbital(Orbital):
    def __init__(
        self,
        position,
        angular_momentum: int,
        magnetic_number: int,
        radial_nodes: int,
        kind_name: str | None = None,
        spin: int = 0,
        **kwargs,
    ) -> None: ...
    @classmethod
    def get_name_from_quantum_numbers(cls, angular_momentum: int, magnetic_number: int | None = None) -> str: ...
    @classmethod
    def get_quantum_numbers_from_name(cls, name: str) -> list[dict]: ...


class NoncollinearHydrogenOrbital(RealhydrogenOrbital):
    def __init__(self, position, angular_momentum: int, magnetic_number: int, radial_nodes: int, spin: float = 0.0, **kwargs) -> None: ...


class SpinorbitHydrogenOrbital(Orbital):
    def __init__(
        self,
        position,
        angular_momentum: int,
        total_angular_momentum: float,
        magnetic_number: float,
        radial_nodes: int,
        kind_name: str | None = None,
        **kwargs,
    ) -> None: ...


class OrbitalData:
    def set_orbitals(self, orbitals) -> None: ...
    def get_orbitals(self, **kwargs) -> list[Orbital]: ...
    def clear_orbitals(self) -> None: ...


class ProjectionData(OrbitalData):
    def set_reference_bandsdata(self, value) -> None: ...
    def get_reference_bandsdata(self): ...
    def set_projectiondata(self, list_of_orbitals, list_of_projections=None, list_of_pdos=None, list_of_energy=None, bands_check=True) -> None: ...
    def get_pdos(self, **kwargs) -> list: ...
    def get_projections(self, **kwargs) -> list: ...
