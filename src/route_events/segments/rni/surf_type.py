from typing import Literal, Annotated, List
from annotated_types import Len
from dataclasses import dataclass


@dataclass
class SurfaceType(object):
    surf_type: int
    category: Literal['paved', 'unpaved']
    surface: Literal['asphal', 'penmac', 'tanah']
    iri_kemantapan: Annotated[List[int], Len(min_length=3, max_length=3)]
    pci_kemantapan: Annotated[List[int], Len(min_length=3, max_length=3)]
    iri_rating: Annotated[List[int], Len(min_length=4, max_length=4)]
    pci_rating: Annotated[List[int], Len(min_length=4, max_length=4)]


_1 = SurfaceType(
    surf_type=1,
    category='unpaved',
    surface='tanah',
    iri_kemantapan=[10, 12, 16],
    pci_kemantapan=[70, 55, 40],
    iri_rating=[4, 9, 13, 14],
    pci_rating=[85, 70, 55, 25]
)

_2 = SurfaceType(
    surf_type=2,
    category='unpaved',
    surface='tanah',
    iri_kemantapan=[10, 12, 16],
    pci_kemantapan=[70, 55, 40],
    iri_rating=[4, 9, 13, 14],
    pci_rating=[85, 70, 55, 25]
)

_3 = SurfaceType(
    surf_type=3,
    category='paved',
    surface='penmac',
    iri_kemantapan=[4, 8, 12],
    pci_kemantapan=[70, 55, 40],
    iri_rating=[4, 9, 13, 14],
    pci_rating=[85, 70, 55, 25]
)

_6 = SurfaceType(
    surf_type=6,
    category='paved',
    surface='penmac',
    iri_kemantapan=[4, 8, 12],
    pci_kemantapan=[70, 55, 40],
    iri_rating=[4, 9, 13, 14],
    pci_rating=[85, 70, 55, 25]
)

_7 = SurfaceType(
    surf_type=7,
    category='paved',
    surface='penmac',
    iri_kemantapan=[4, 8, 12],
    pci_kemantapan=[70, 55, 40],
    iri_rating=[4, 9, 13, 14],
    pci_rating=[85, 70, 55, 25]
)

_4 = SurfaceType(
    surf_type=4,
    category='paved',
    surface='asphal',
    iri_kemantapan=[4, 8, 12],
    pci_kemantapan=[70, 55, 40],
    iri_rating=[4, 9, 13, 14],
    pci_rating=[85, 70, 55, 25]
)

_5 = SurfaceType(
    surf_type=5,
    category='paved',
    surface='asphal',
    iri_kemantapan=[4, 8, 12],
    pci_kemantapan=[70, 55, 40],
    iri_rating=[4, 9, 13, 14],
    pci_rating=[85, 70, 55, 25]
)

_8 = SurfaceType(
    surf_type=8,
    category='paved',
    surface='asphal',
    iri_kemantapan=[4, 8, 12],
    pci_kemantapan=[70, 55, 40],
    iri_rating=[4, 9, 13, 14],
    pci_rating=[85, 70, 55, 25]
)

_9 = SurfaceType(
    surf_type=9,
    category='paved',
    surface='asphal',
    iri_kemantapan=[4, 8, 12],
    pci_kemantapan=[70, 55, 40],
    iri_rating=[4, 9, 13, 14],
    pci_rating=[85, 70, 55, 25]
)

_10 = SurfaceType(
    surf_type=10,
    category='paved',
    surface='asphal',
    iri_kemantapan=[4, 8, 12],
    pci_kemantapan=[70, 55, 40],
    iri_rating=[4, 9, 13, 14],
    pci_rating=[85, 70, 55, 25]
)

_11 = SurfaceType(
    surf_type=11,
    category='paved',
    surface='asphal',
    iri_kemantapan=[4, 8, 12],
    pci_kemantapan=[70, 55, 40],
    iri_rating=[4, 9, 13, 14],
    pci_rating=[85, 70, 55, 25]
)

_12 = SurfaceType(
    surf_type=12,
    category='paved',
    surface='asphal',
    iri_kemantapan=[4, 8, 12],
    pci_kemantapan=[70, 55, 40],
    iri_rating=[4, 9, 13, 14],
    pci_rating=[85, 70, 55, 25]
)

_13 = SurfaceType(
    surf_type=13,
    category='paved',
    surface='asphal',
    iri_kemantapan=[4, 8, 12],
    pci_kemantapan=[70, 55, 40],
    iri_rating=[4, 9, 13, 14],
    pci_rating=[85, 70, 55, 25]
)

_14 = SurfaceType(
    surf_type=14,
    category='paved',
    surface='asphal',
    iri_kemantapan=[4, 8, 12],
    pci_kemantapan=[70, 55, 40],
    iri_rating=[4, 9, 13, 14],
    pci_rating=[85, 70, 55, 25]
)

_15 = SurfaceType(
    surf_type=15,
    category='paved',
    surface='asphal',
    iri_kemantapan=[4, 8, 12],
    pci_kemantapan=[70, 55, 40],
    iri_rating=[4, 9, 13, 14],
    pci_rating=[85, 70, 55, 25]
)

_16 = SurfaceType(
    surf_type=16,
    category='paved',
    surface='asphal',
    iri_kemantapan=[4, 8, 12],
    pci_kemantapan=[70, 55, 40],
    iri_rating=[4, 9, 13, 14],
    pci_rating=[85, 70, 55, 25]
)

_17 = SurfaceType(
    surf_type=17,
    category='paved',
    surface='asphal',
    iri_kemantapan=[4, 8, 12],
    pci_kemantapan=[70, 55, 40],
    iri_rating=[4, 9, 13, 14],
    pci_rating=[85, 70, 55, 25]
)

_18 = SurfaceType(
    surf_type=18,
    category='paved',
    surface='asphal',
    iri_kemantapan=[4, 8, 12],
    pci_kemantapan=[70, 55, 40],
    iri_rating=[4, 9, 13, 14],
    pci_rating=[85, 70, 55, 25]
)

_19 = SurfaceType(
    surf_type=19,
    category='paved',
    surface='asphal',
    iri_kemantapan=[4, 8, 12],
    pci_kemantapan=[70, 55, 40],
    iri_rating=[4, 9, 13, 14],
    pci_rating=[85, 70, 55, 25]
)

_20 = SurfaceType(
    surf_type=20,
    category='paved',
    surface='asphal',
    iri_kemantapan=[4, 8, 12],
    pci_kemantapan=[70, 55, 40],
    iri_rating=[4, 9, 13, 14],
    pci_rating=[85, 70, 55, 25]
)

_21 = SurfaceType(
    surf_type=21,
    category='paved',
    surface='asphal',
    iri_kemantapan=[4, 8, 12],
    pci_kemantapan=[70, 55, 40],
    iri_rating=[4, 9, 13, 14],

    pci_rating=[85, 70, 55, 25]
)

_surface_types = [
    _1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15,
    _16, _17, _18, _19, _20, _21
]