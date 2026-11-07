"""La tasa de cierre es anualizada (como la AFR de los discos), no una proporción de cerrados."""
import pytest

from queries import tasa_cierre


def test_un_cierre_en_un_anio_negocio_es_100_por_ciento():
    assert tasa_cierre(1, 365) == 100.0


def test_formula_anualizada():
    # 10 cierres en 3.650 días-negocio (10 años-negocio) = 100 % ; 5 en 36.500 = 5 %
    assert tasa_cierre(10, 3650) == pytest.approx(100.0)
    assert tasa_cierre(5, 36500) == pytest.approx(5.0)


def test_no_es_proporcion_de_cerrados():
    # 2 de 4 negocios cerrados (50 %), pero con 4.000 días-negocio en total la tasa anual es 18,25 %
    assert tasa_cierre(2, 4000) == pytest.approx(18.25)
    assert tasa_cierre(2, 4000) != 50.0


def test_igual_a_la_afr_de_backblaze():
    # 1.498 fallas en 31.553.350 días-disco = 1,73 % (Backblaze Q2 2026)
    assert round(tasa_cierre(1498, 31553350), 2) == 1.73


@pytest.mark.parametrize("dias", [0, -5, None])
def test_sin_exposicion_no_hay_tasa(dias):
    assert tasa_cierre(3, dias) is None
