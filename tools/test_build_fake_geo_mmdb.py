"""Roundtrip tests for the fake geo + ISP mmdb generator."""

from pathlib import Path

import maxminddb
import pytest

import build_fake_geo_mmdb as m


@pytest.fixture
def geo_db(tmp_path):
    path = tmp_path / "GeoLite2-City.mmdb"
    m.write_mmdb(path, "geo", "GeoLite2-City", "test")
    return maxminddb.open_database(str(path))


@pytest.fixture
def isp_db(tmp_path):
    path = tmp_path / "GeoIP2-ISP.mmdb"
    m.write_mmdb(path, "isp", "GeoIP2-ISP", "test")
    return maxminddb.open_database(str(path))


def test_geo_db_mountain_view(geo_db):
    rec = geo_db.get("8.8.42.17")
    assert rec["country"]["iso_code"] == "US"
    assert rec["city"]["names"]["en"] == "Mountain View"
    assert rec["postal"]["code"] == "94043"
    assert rec["location"]["time_zone"] == "America/Los_Angeles"
    assert rec["subdivisions"][0]["iso_code"] == "CA"
    assert 37.0 < rec["location"]["latitude"] < 38.0


def test_geo_db_frankfurt(geo_db):
    rec = geo_db.get("178.128.42.17")
    assert rec["country"]["iso_code"] == "DE"
    assert rec["city"]["names"]["en"] == "Frankfurt am Main"
    assert rec["location"]["time_zone"] == "Europe/Berlin"


def test_geo_db_sydney(geo_db):
    rec = geo_db.get("3.104.99.99")
    assert rec["country"]["iso_code"] == "AU"
    assert rec["city"]["names"]["en"] == "Sydney"
    assert rec["location"]["longitude"] > 150


def test_geo_db_default_falls_back_to_ashburn(geo_db):
    rec = geo_db.get("1.2.3.4")
    assert rec["country"]["iso_code"] == "US"
    assert rec["city"]["names"]["en"] == "Ashburn"


def test_isp_db_google(isp_db):
    rec = isp_db.get("8.8.42.17")
    assert rec["isp"] == "Google LLC"
    assert rec["organization"] == "Google LLC"
    assert rec["autonomous_system_number"] == 15169


def test_isp_db_hetzner(isp_db):
    rec = isp_db.get("94.130.99.99")
    assert rec["isp"] == "Hetzner Online GmbH"
    assert rec["autonomous_system_number"] == 24940


def test_isp_db_default(isp_db):
    rec = isp_db.get("1.2.3.4")
    assert rec["isp"] == "Unknown"
    assert rec["autonomous_system_number"] == 0


def test_every_loader_block_has_entry():
    """Every /16 IP prefix used by the loader must have both geo and ISP records."""
    for cidr, data in m.BLOCKS.items():
        assert set(data.keys()) == {"geo", "isp"}, cidr
        assert data["geo"]["country"]["iso_code"], cidr
        assert data["geo"]["city"]["names"]["en"], cidr
        assert data["geo"]["location"]["time_zone"], cidr
        assert data["isp"]["isp"], cidr
        assert data["isp"]["autonomous_system_number"] >= 0, cidr
