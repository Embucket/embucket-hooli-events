"""Generate fake MaxMind-format geo and ISP .mmdb databases for Hooli Events.

The Snowplow ip_lookups enrichment reads these at runtime. Each entry covers
one of the loader's 18 fake IP blocks (see simulator/simulate.py
PUBLIC_IP_BLOCKS) plus a default /0 for anything else. Re-run this script
whenever PUBLIC_IP_BLOCKS changes and commit the two .mmdb files.
"""

from pathlib import Path

from mmdb_writer import MMDBWriter
from netaddr import IPSet


BLOCKS = {
    "8.8.0.0/16": {
        "geo": {
            "country": {"iso_code": "US", "names": {"en": "United States"}},
            "subdivisions": [{"iso_code": "CA", "names": {"en": "California"}}],
            "city": {"names": {"en": "Mountain View"}},
            "postal": {"code": "94043"},
            "location": {"latitude": 37.4056, "longitude": -122.0775,
                         "time_zone": "America/Los_Angeles", "accuracy_radius": 20},
        },
        "isp": {"isp": "Google LLC", "organization": "Google LLC",
                "autonomous_system_number": 15169,
                "autonomous_system_organization": "GOOGLE"},
    },
    "13.107.0.0/16": {
        "geo": {
            "country": {"iso_code": "US", "names": {"en": "United States"}},
            "subdivisions": [{"iso_code": "WA", "names": {"en": "Washington"}}],
            "city": {"names": {"en": "Redmond"}},
            "postal": {"code": "98052"},
            "location": {"latitude": 47.6740, "longitude": -122.1215,
                         "time_zone": "America/Los_Angeles", "accuracy_radius": 20},
        },
        "isp": {"isp": "Microsoft Corporation", "organization": "Microsoft Corporation",
                "autonomous_system_number": 8075,
                "autonomous_system_organization": "MICROSOFT-CORP-MSN-AS-BLOCK"},
    },
    "52.95.0.0/16": {
        "geo": {
            "country": {"iso_code": "US", "names": {"en": "United States"}},
            "subdivisions": [{"iso_code": "VA", "names": {"en": "Virginia"}}],
            "city": {"names": {"en": "Ashburn"}},
            "postal": {"code": "20147"},
            "location": {"latitude": 39.0438, "longitude": -77.4874,
                         "time_zone": "America/New_York", "accuracy_radius": 20},
        },
        "isp": {"isp": "Amazon.com, Inc.", "organization": "Amazon Technologies Inc.",
                "autonomous_system_number": 16509,
                "autonomous_system_organization": "AMAZON-02"},
    },
    "94.130.0.0/16": {
        "geo": {
            "country": {"iso_code": "DE", "names": {"en": "Germany"}},
            "subdivisions": [{"iso_code": "SN", "names": {"en": "Saxony"}}],
            "city": {"names": {"en": "Falkenstein"}},
            "postal": {"code": "08223"},
            "location": {"latitude": 50.4779, "longitude": 12.3713,
                         "time_zone": "Europe/Berlin", "accuracy_radius": 20},
        },
        "isp": {"isp": "Hetzner Online GmbH", "organization": "Hetzner Online GmbH",
                "autonomous_system_number": 24940,
                "autonomous_system_organization": "HETZNER-AS"},
    },
    "178.128.0.0/16": {
        "geo": {
            "country": {"iso_code": "DE", "names": {"en": "Germany"}},
            "subdivisions": [{"iso_code": "HE", "names": {"en": "Hesse"}}],
            "city": {"names": {"en": "Frankfurt am Main"}},
            "postal": {"code": "60311"},
            "location": {"latitude": 50.1109, "longitude": 8.6821,
                         "time_zone": "Europe/Berlin", "accuracy_radius": 20},
        },
        "isp": {"isp": "DigitalOcean, LLC", "organization": "DigitalOcean, LLC",
                "autonomous_system_number": 14061,
                "autonomous_system_organization": "DIGITALOCEAN-ASN"},
    },
    "195.149.0.0/16": {
        "geo": {
            "country": {"iso_code": "SE", "names": {"en": "Sweden"}},
            "subdivisions": [{"iso_code": "AB", "names": {"en": "Stockholm County"}}],
            "city": {"names": {"en": "Stockholm"}},
            "postal": {"code": "11129"},
            "location": {"latitude": 59.3293, "longitude": 18.0686,
                         "time_zone": "Europe/Stockholm", "accuracy_radius": 20},
        },
        "isp": {"isp": "Bahnhof AB", "organization": "Bahnhof AB",
                "autonomous_system_number": 8473,
                "autonomous_system_organization": "BAHNHOF"},
    },
    "217.160.0.0/16": {
        "geo": {
            "country": {"iso_code": "DE", "names": {"en": "Germany"}},
            "subdivisions": [{"iso_code": "NW", "names": {"en": "North Rhine-Westphalia"}}],
            "city": {"names": {"en": "Karlsruhe"}},
            "postal": {"code": "76131"},
            "location": {"latitude": 49.0069, "longitude": 8.4037,
                         "time_zone": "Europe/Berlin", "accuracy_radius": 20},
        },
        "isp": {"isp": "IONOS SE", "organization": "IONOS SE",
                "autonomous_system_number": 8560,
                "autonomous_system_organization": "ONEANDONE-AS"},
    },
    "34.102.0.0/16": {
        "geo": {
            "country": {"iso_code": "US", "names": {"en": "United States"}},
            "subdivisions": [{"iso_code": "IA", "names": {"en": "Iowa"}}],
            "city": {"names": {"en": "Council Bluffs"}},
            "postal": {"code": "51501"},
            "location": {"latitude": 41.2619, "longitude": -95.8608,
                         "time_zone": "America/Chicago", "accuracy_radius": 20},
        },
        "isp": {"isp": "Google LLC", "organization": "Google Cloud",
                "autonomous_system_number": 15169,
                "autonomous_system_organization": "GOOGLE"},
    },
    "34.120.0.0/16": {
        "geo": {
            "country": {"iso_code": "US", "names": {"en": "United States"}},
            "subdivisions": [{"iso_code": "SC", "names": {"en": "South Carolina"}}],
            "city": {"names": {"en": "Moncks Corner"}},
            "postal": {"code": "29461"},
            "location": {"latitude": 33.1960, "longitude": -80.0139,
                         "time_zone": "America/New_York", "accuracy_radius": 20},
        },
        "isp": {"isp": "Google LLC", "organization": "Google Cloud",
                "autonomous_system_number": 15169,
                "autonomous_system_organization": "GOOGLE"},
    },
    "52.230.0.0/16": {
        "geo": {
            "country": {"iso_code": "NL", "names": {"en": "Netherlands"}},
            "subdivisions": [{"iso_code": "NH", "names": {"en": "North Holland"}}],
            "city": {"names": {"en": "Amsterdam"}},
            "postal": {"code": "1012"},
            "location": {"latitude": 52.3676, "longitude": 4.9041,
                         "time_zone": "Europe/Amsterdam", "accuracy_radius": 20},
        },
        "isp": {"isp": "Microsoft Corporation", "organization": "Microsoft Azure",
                "autonomous_system_number": 8075,
                "autonomous_system_organization": "MICROSOFT-CORP-MSN-AS-BLOCK"},
    },
    "13.228.0.0/16": {
        "geo": {
            "country": {"iso_code": "SG", "names": {"en": "Singapore"}},
            "subdivisions": [{"iso_code": "01", "names": {"en": "Central Singapore"}}],
            "city": {"names": {"en": "Singapore"}},
            "postal": {"code": "018989"},
            "location": {"latitude": 1.3521, "longitude": 103.8198,
                         "time_zone": "Asia/Singapore", "accuracy_radius": 20},
        },
        "isp": {"isp": "Amazon.com, Inc.", "organization": "Amazon Technologies Inc.",
                "autonomous_system_number": 16509,
                "autonomous_system_organization": "AMAZON-02"},
    },
    "13.251.0.0/16": {
        "geo": {
            "country": {"iso_code": "SG", "names": {"en": "Singapore"}},
            "subdivisions": [{"iso_code": "01", "names": {"en": "Central Singapore"}}],
            "city": {"names": {"en": "Singapore"}},
            "postal": {"code": "018989"},
            "location": {"latitude": 1.3521, "longitude": 103.8198,
                         "time_zone": "Asia/Singapore", "accuracy_radius": 20},
        },
        "isp": {"isp": "Amazon.com, Inc.", "organization": "Amazon Technologies Inc.",
                "autonomous_system_number": 16509,
                "autonomous_system_organization": "AMAZON-02"},
    },
    "3.112.0.0/16": {
        "geo": {
            "country": {"iso_code": "JP", "names": {"en": "Japan"}},
            "subdivisions": [{"iso_code": "13", "names": {"en": "Tokyo"}}],
            "city": {"names": {"en": "Tokyo"}},
            "postal": {"code": "100-0001"},
            "location": {"latitude": 35.6895, "longitude": 139.6917,
                         "time_zone": "Asia/Tokyo", "accuracy_radius": 20},
        },
        "isp": {"isp": "Amazon.com, Inc.", "organization": "Amazon Technologies Inc.",
                "autonomous_system_number": 16509,
                "autonomous_system_organization": "AMAZON-02"},
    },
    "18.130.0.0/16": {
        "geo": {
            "country": {"iso_code": "GB", "names": {"en": "United Kingdom"}},
            "subdivisions": [{"iso_code": "ENG", "names": {"en": "England"}}],
            "city": {"names": {"en": "London"}},
            "postal": {"code": "EC1A"},
            "location": {"latitude": 51.5074, "longitude": -0.1278,
                         "time_zone": "Europe/London", "accuracy_radius": 20},
        },
        "isp": {"isp": "Amazon.com, Inc.", "organization": "Amazon Technologies Inc.",
                "autonomous_system_number": 16509,
                "autonomous_system_organization": "AMAZON-02"},
    },
    "15.221.0.0/16": {
        "geo": {
            "country": {"iso_code": "CA", "names": {"en": "Canada"}},
            "subdivisions": [{"iso_code": "QC", "names": {"en": "Quebec"}}],
            "city": {"names": {"en": "Montreal"}},
            "postal": {"code": "H2X"},
            "location": {"latitude": 45.5017, "longitude": -73.5673,
                         "time_zone": "America/Toronto", "accuracy_radius": 20},
        },
        "isp": {"isp": "Amazon.com, Inc.", "organization": "Amazon Technologies Inc.",
                "autonomous_system_number": 16509,
                "autonomous_system_organization": "AMAZON-02"},
    },
    "3.104.0.0/16": {
        "geo": {
            "country": {"iso_code": "AU", "names": {"en": "Australia"}},
            "subdivisions": [{"iso_code": "NSW", "names": {"en": "New South Wales"}}],
            "city": {"names": {"en": "Sydney"}},
            "postal": {"code": "2000"},
            "location": {"latitude": -33.8688, "longitude": 151.2093,
                         "time_zone": "Australia/Sydney", "accuracy_radius": 20},
        },
        "isp": {"isp": "Amazon.com, Inc.", "organization": "Amazon Technologies Inc.",
                "autonomous_system_number": 16509,
                "autonomous_system_organization": "AMAZON-02"},
    },
    "18.228.0.0/16": {
        "geo": {
            "country": {"iso_code": "BR", "names": {"en": "Brazil"}},
            "subdivisions": [{"iso_code": "SP", "names": {"en": "São Paulo"}}],
            "city": {"names": {"en": "São Paulo"}},
            "postal": {"code": "01000-000"},
            "location": {"latitude": -23.5505, "longitude": -46.6333,
                         "time_zone": "America/Sao_Paulo", "accuracy_radius": 20},
        },
        "isp": {"isp": "Amazon.com, Inc.", "organization": "Amazon Technologies Inc.",
                "autonomous_system_number": 16509,
                "autonomous_system_organization": "AMAZON-02"},
    },
    "15.161.0.0/16": {
        "geo": {
            "country": {"iso_code": "IT", "names": {"en": "Italy"}},
            "subdivisions": [{"iso_code": "25", "names": {"en": "Lombardy"}}],
            "city": {"names": {"en": "Milan"}},
            "postal": {"code": "20121"},
            "location": {"latitude": 45.4642, "longitude": 9.1900,
                         "time_zone": "Europe/Rome", "accuracy_radius": 20},
        },
        "isp": {"isp": "Amazon.com, Inc.", "organization": "Amazon Technologies Inc.",
                "autonomous_system_number": 16509,
                "autonomous_system_organization": "AMAZON-02"},
    },
}


# Default entry for IPs outside the 18 loader blocks.
DEFAULT = {
    "geo": {
        "country": {"iso_code": "US", "names": {"en": "United States"}},
        "subdivisions": [{"iso_code": "VA", "names": {"en": "Virginia"}}],
        "city": {"names": {"en": "Ashburn"}},
        "postal": {"code": "20147"},
        "location": {"latitude": 39.0438, "longitude": -77.4874,
                     "time_zone": "America/New_York", "accuracy_radius": 100},
    },
    "isp": {"isp": "Unknown", "organization": "Unknown",
            "autonomous_system_number": 0,
            "autonomous_system_organization": "UNKNOWN"},
}


def write_mmdb(path, lookup_key, database_type, description_en):
    writer = MMDBWriter(ip_version=4, database_type=database_type,
                        description={"en": description_en})
    # Default entry registered FIRST as two halved IPv4 ranges — mmdb-writer
    # can't represent a literal /0, and later insert_network calls for the
    # specific BLOCKS override the default's matching bits. Two halves
    # blanket-cover every IPv4 except those we subsequently pin.
    writer.insert_network(IPSet(["0.0.0.0/1"]), DEFAULT[lookup_key])
    writer.insert_network(IPSet(["128.0.0.0/1"]), DEFAULT[lookup_key])
    for cidr, data in BLOCKS.items():
        writer.insert_network(IPSet([cidr]), data[lookup_key])
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    writer.to_db_file(str(path))


def main():
    root = Path(__file__).resolve().parent.parent
    geo_path = root / "config" / "maxmind" / "GeoLite2-City.mmdb"
    isp_path = root / "config" / "maxmind" / "GeoIP2-ISP.mmdb"
    write_mmdb(geo_path, "geo", "GeoLite2-City", "Hooli fake geo")
    write_mmdb(isp_path, "isp", "GeoIP2-ISP", "Hooli fake ISP")
    print(f"wrote {geo_path}")
    print(f"wrote {isp_path}")


if __name__ == "__main__":
    main()
