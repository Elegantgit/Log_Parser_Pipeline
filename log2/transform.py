
import xml.etree.ElementTree as ET
import logging
from datetime import datetime


def extract_top_level(record):
    return {
        "id": record.get("_id", {}).get("$oid"),
        "action": record.get("action"),
        "success": record.get("success"),
        "gateway": record.get("gateway"),
        "ref": record.get("ref"),
        "service": record.get("service"),
        "time": record.get("time"),
        "created": record.get("created", {}).get("$date"),
        "response": record.get("response")
    }



TARGET_FIELDS = {
    "response", "amount", "confirmationTime",
    "customerAddress", "customerMeterNumber",
    "debtAmount", "initiationTime",
    "status", "units", "unitsType",
    "value", "vat",
    "desc", "retn" 
}

def parse_xml(xml_string):
    if not xml_string:
        return {}

    try:
        root = ET.fromstring(xml_string)

        def strip_ns(tag):
            return tag.split("}")[-1]

        data = {}

        for elem in root.iter():
            tag = strip_ns(elem.tag)

            if tag in TARGET_FIELDS:
                text = elem.text.strip() if elem.text else None
                data[tag] = text

        return data

    except ET.ParseError:
        logging.warning("Invalid XML encountered")
        return {}



def normalize_response(data):
    if "desc" in data:
        data["status"] = data["desc"]
    if "retn" in data:
        data["response_code"] = data["retn"]

    return data


def to_float(val):
    try:
        return float(val)
    except:
        return None

def to_bool(val):
    return str(val).lower() == "true"

def to_timestamp(val):
    try:
        return datetime.fromisoformat(val)
    except:
        return None

def clean_record(record):
    record["amount"] = to_float(record.get("amount"))
    record["debtAmount"] = to_float(record.get("debtAmount"))
    record["vat"] = to_float(record.get("vat"))
    record["units"] = to_float(record.get("units"))

    record["success"] = to_bool(record.get("success"))

    record["created"] = to_timestamp(record.get("created"))
    record["time"] = to_timestamp(record.get("time"))
    record["confirmationTime"] = to_timestamp(record.get("confirmationTime"))
    record["initiationTime"] = to_timestamp(record.get("initiationTime"))

    return record



def transform_record(record):
    top = extract_top_level(record)
    xml = parse_xml(record.get("response"))


    combined = {**top, **xml}
    combined = normalize_response(combined)

    #combined = {**top, **xml}
    return clean_record(combined)


