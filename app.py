import os
import re
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Union

import requests
import pandas as pd
import jwt
import bcrypt
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Depends, Query, Request, status
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
import pymongo
from pymongo import MongoClient

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("brick_kiln_monitor")

# Load environment variables
load_dotenv(override=True)

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb+srv://<username>:<password>@cluster.mongodb.net/?retryWrites=true&w=majority")
DATABASE_NAME = os.getenv("DATABASE_NAME", "brick_kiln_db")
RAW_COLLECTION = os.getenv("RAW_COLLECTION", "raw_submissions")
CLEAN_COLLECTION = os.getenv("CLEAN_COLLECTION", "cleaned_submissions")
AUDIT_COLLECTION = os.getenv("AUDIT_COLLECTION", "audit_logs")
SYNC_COLLECTION = "sync_logs"

ODK_BASE_URL = os.getenv("ODK_BASE_URL", "https://odk.yourdomain.com").rstrip("/")
ODK_EMAIL = os.getenv("ODK_EMAIL", "admin@example.com")
ODK_PASSWORD = os.getenv("ODK_PASSWORD", "your_password")
ODK_PROJECT_ID = os.getenv("ODK_PROJECT_ID", "5")
ODK_FORM_ID = os.getenv("ODK_FORM_ID", "brick_kiln_survey")

JWT_SECRET = os.getenv("JWT_SECRET", "super_secret_jwt_key_2026")
JWT_ALGORITHM = "HS256"
PORT = int(os.getenv("PORT", 8000))

# Pre-hashed passwords for hardcoded users
USERS_DB = {
    "eayashen": {
        "username": "eayashen",
        "password_hash": bcrypt.hashpw(b"kiln1234", bcrypt.gensalt()).decode("utf-8"),
        "display_name": "Eayashen",
        "role": "Quality Architect & Admin",
        "avatar_color": "indigo"
    },
    "meftah": {
        "username": "meftah",
        "password_hash": bcrypt.hashpw(b"kiln4321", bcrypt.gensalt()).decode("utf-8"),
        "display_name": "Meftah",
        "role": "Data Quality Reviewer",
        "avatar_color": "emerald"
    }
}

# ------------------------------------------------------------------------------
# Resilient Database Wrapper (Handles Atlas + Local Fallback if Mongo unreachable)
# ------------------------------------------------------------------------------
class ResilientDBStore:
    def __init__(self):
        self.is_connected_to_mongo = False
        self.mongo_client = None
        self.db = None
        self.fallback_file = os.path.join(os.path.dirname(__file__), "brick_kiln_local_store.json")
        self.memory_store = {
            RAW_COLLECTION: {},
            CLEAN_COLLECTION: {},
            AUDIT_COLLECTION: [],
            SYNC_COLLECTION: []
        }
        self._init_connection()

    def _init_connection(self):
        # Check if URI contains placeholders
        if "<username>" in MONGODB_URI or "<password>" in MONGODB_URI:
            logger.warning("MONGODB_URI contains placeholder credentials (<username>/<password>). Starting with resilient local persistence store.")
            self._load_fallback()
            return

        try:
            client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=2500)
            client.admin.command('ping')
            self.mongo_client = client
            self.db = client[DATABASE_NAME]
            self.is_connected_to_mongo = True
            logger.info("Successfully connected to live MongoDB: %s", DATABASE_NAME)
        except Exception as e:
            logger.warning("Unable to connect to MongoDB (%s). Using local persistent store.", str(e))
            self.is_connected_to_mongo = False
            self._load_fallback()

    def _load_fallback(self):
        if os.path.exists(self.fallback_file):
            try:
                with open(self.fallback_file, "r") as f:
                    data = json.load(f)
                    self.memory_store = data
                    logger.info("Loaded local store from %s", self.fallback_file)
            except Exception as e:
                logger.error("Failed to load local store file: %s", str(e))

    def _save_fallback(self):
        if not self.is_connected_to_mongo:
            try:
                with open(self.fallback_file, "w") as f:
                    json.dump(self.memory_store, f, indent=2, default=str)
            except Exception as e:
                logger.error("Failed to save local store: %s", str(e))

    # Generic helpers for collections
    def normalize_doc(self, doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not doc:
            return None
        res = dict(doc)
        # If document has nested 'data' dictionary from ODK Central/Atlas, flatten/merge it
        if "data" in res and isinstance(res["data"], dict):
            for k, v in res["data"].items():
                if k not in res or res[k] is None:
                    res[k] = v
        inst = (
            res.get("meta-instanceID") or 
            res.get("instance_id") or 
            res.get("__id") or 
            res.get("KEY") or 
            res.get("submission_id")
        )
        if inst:
            res["meta-instanceID"] = str(inst)
        return res

    def get_raw_all(self) -> List[Dict[str, Any]]:
        if self.is_connected_to_mongo:
            docs = list(self.db[RAW_COLLECTION].find({}, {"_id": 0}))
            return [self.normalize_doc(d) for d in docs if d]
        return [self.normalize_doc(d) for d in self.memory_store[RAW_COLLECTION].values() if d]

    def get_clean_all(self) -> List[Dict[str, Any]]:
        if self.is_connected_to_mongo:
            docs = list(self.db[CLEAN_COLLECTION].find({}, {"_id": 0}))
            return [self.normalize_doc(d) for d in docs if d]
        return [self.normalize_doc(d) for d in self.memory_store[CLEAN_COLLECTION].values() if d]

    def get_clean_by_instance(self, instance_id: str) -> Optional[Dict[str, Any]]:
        if self.is_connected_to_mongo:
            query = {
                "$or": [
                    {"meta-instanceID": instance_id},
                    {"instance_id": instance_id},
                    {"data.__id": instance_id},
                    {"data.meta-instanceID": instance_id},
                    {"KEY": instance_id}
                ]
            }
            doc = self.db[CLEAN_COLLECTION].find_one(query, {"_id": 0})
            return self.normalize_doc(doc)
        rec = self.memory_store[CLEAN_COLLECTION].get(instance_id)
        return self.normalize_doc(rec)

    def upsert_raw(self, doc: Dict[str, Any]):
        inst_id = (
            doc.get("instance_id") or 
            doc.get("meta-instanceID") or 
            doc.get("__id") or 
            doc.get("KEY") or 
            doc.get("instanceId")
        )
        if not inst_id:
            return
        inst_str = str(inst_id)
        doc["instance_id"] = inst_str
        doc["meta-instanceID"] = inst_str
        doc.setdefault("form_id", ODK_FORM_ID)
        doc.setdefault("synced_at", datetime.now(timezone.utc))

        if self.is_connected_to_mongo:
            query = {"instance_id": inst_str}
            self.db[RAW_COLLECTION].update_one(query, {"$set": doc}, upsert=True)
        else:
            self.memory_store[RAW_COLLECTION][inst_str] = doc
            self._save_fallback()

    def upsert_clean(self, doc: Dict[str, Any]):
        inst_id = (
            doc.get("instance_id") or 
            doc.get("meta-instanceID") or 
            doc.get("__id") or 
            doc.get("KEY") or 
            doc.get("instanceId")
        )
        if not inst_id:
            return
        inst_str = str(inst_id)
        doc["instance_id"] = inst_str
        doc["meta-instanceID"] = inst_str
        doc.setdefault("form_id", ODK_FORM_ID)

        if self.is_connected_to_mongo:
            query = {"instance_id": inst_str}
            existing = self.db[CLEAN_COLLECTION].find_one(query)
            if existing:
                modified = existing.get("_modified_fields", {})
                resolved = existing.get("_resolved_flags", [])
                doc["_modified_fields"] = modified
                doc["_resolved_flags"] = resolved
                for field, val in modified.items():
                    doc[field] = val
                    if "data" in doc and isinstance(doc["data"], dict):
                        doc["data"][field] = val
            self.db[CLEAN_COLLECTION].update_one(query, {"$set": doc}, upsert=True)
        else:
            existing = self.memory_store[CLEAN_COLLECTION].get(inst_str)
            if existing:
                modified = existing.get("_modified_fields", {})
                resolved = existing.get("_resolved_flags", [])
                doc["_modified_fields"] = modified
                doc["_resolved_flags"] = resolved
                for field, val in modified.items():
                    doc[field] = val
            self.memory_store[CLEAN_COLLECTION][inst_str] = doc
            self._save_fallback()

    def update_clean_record(self, instance_id: str, updates: Dict[str, Any], resolved_flag: Optional[Union[str, List[str]]] = None):
        if self.is_connected_to_mongo:
            query = {
                "$or": [
                    {"meta-instanceID": instance_id},
                    {"instance_id": instance_id},
                    {"data.__id": instance_id},
                    {"data.meta-instanceID": instance_id},
                    {"KEY": instance_id}
                ]
            }
            current = self.db[CLEAN_COLLECTION].find_one(query)
            if not current:
                return False
            resolved = list(current.get("_resolved_flags", []))
            if resolved_flag:
                flags_list = [resolved_flag] if isinstance(resolved_flag, str) else resolved_flag
                for fl in flags_list:
                    if fl and fl not in resolved:
                        resolved.append(fl)
            
            modified = current.get("_modified_fields", {})
            for k, v in updates.items():
                if not k.startswith("_"):
                    modified[k] = v

            set_payload = {
                **updates, 
                "_resolved_flags": resolved, 
                "_modified_fields": modified, 
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
            
            # If document has nested data dict, update nested path as well
            if "data" in current and isinstance(current["data"], dict):
                for k, v in updates.items():
                    if not k.startswith("_"):
                        set_payload[f"data.{k}"] = v

            self.db[CLEAN_COLLECTION].update_one(query, {"$set": set_payload})
            return True
        else:
            if instance_id not in self.memory_store[CLEAN_COLLECTION]:
                return False
            rec = self.memory_store[CLEAN_COLLECTION][instance_id]
            resolved = rec.setdefault("_resolved_flags", [])
            if resolved_flag:
                flags_list = [resolved_flag] if isinstance(resolved_flag, str) else resolved_flag
                for fl in flags_list:
                    if fl and fl not in resolved:
                        resolved.append(fl)
            
            modified = rec.setdefault("_modified_fields", {})
            for k, v in updates.items():
                if not k.startswith("_"):
                    modified[k] = v
                rec[k] = v
            rec["updated_at"] = datetime.now(timezone.utc).isoformat()
            self._save_fallback()
            return True

    def add_audit_log(self, log_entry: Dict[str, Any]):
        log_entry["created_at"] = datetime.now(timezone.utc).isoformat()
        if self.is_connected_to_mongo:
            self.db[AUDIT_COLLECTION].insert_one(log_entry)
        else:
            self.memory_store[AUDIT_COLLECTION].insert(0, log_entry)
            self._save_fallback()

    def get_audit_logs(self, limit: int = 200) -> List[Dict[str, Any]]:
        if self.is_connected_to_mongo:
            return list(self.db[AUDIT_COLLECTION].find({}, {"_id": 0}).sort("created_at", pymongo.DESCENDING).limit(limit))
        return self.memory_store[AUDIT_COLLECTION][:limit]

    def add_sync_log(self, sync_entry: Dict[str, Any]):
        import uuid
        now_iso = datetime.now(timezone.utc).isoformat()
        sync_entry.setdefault("sync_id", f"sync_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:6]}")
        sync_entry.setdefault("start_time", now_iso)
        sync_entry["timestamp"] = now_iso
        if self.is_connected_to_mongo:
            self.db[SYNC_COLLECTION].insert_one(sync_entry)
        else:
            self.memory_store[SYNC_COLLECTION].insert(0, sync_entry)
            self._save_fallback()

    def get_sync_logs(self, limit: int = 50) -> List[Dict[str, Any]]:
        if self.is_connected_to_mongo:
            return list(self.db[SYNC_COLLECTION].find({}, {"_id": 0}).sort("timestamp", pymongo.DESCENDING).limit(limit))
        return self.memory_store[SYNC_COLLECTION][:limit]

    def seed_initial_records(self, sample_records: List[Dict[str, Any]]):
        count = len(self.get_clean_all())
        if count == 0:
            logger.info("Initializing database with %d realistic sample submissions.", len(sample_records))
            for rec in sample_records:
                self.upsert_raw(dict(rec))
                self.upsert_clean(dict(rec))
            self.add_sync_log({
                "status": "INITIAL_SEED",
                "records_synced": len(sample_records),
                "message": "Initialized repository with benchmark submissions and validation test cases.",
                "user": "system"
            })

db_store = ResilientDBStore()

# ------------------------------------------------------------------------------
# Benchmark Seed Data
# ------------------------------------------------------------------------------
INITIAL_SAMPLE_DATA: List[Dict[str, Any]] = [
    {
        "SubmissionDate": "2026-09-20T06:30:38.355Z",
        "survey_date": "2026-09-20",
        "interviewer": "Yasin",
        "division": "1",
        "district": "06",
        "kiln_number": "002",
        "kiln_unique_id": "106002",
        "kiln_id_display": "106002",
        "worker_name": "Imran",
        "worker_age": 26,
        "job_zone": "firing",
        "worker_unique_id": "106002_F",
        "worker_id_display": "106002_F",
        "experience_years": 5.0,
        "daily_hours": 8,
        "ppe_used": "gloves glasses",
        "resp_symptoms": "sometimes",
        "heat_exhaustion": "no",
        "drinking_water_access": "no",
        "medical_aid_access": "yes",
        "monthly_income": 5000,
        "worksite_location-Latitude": 23.777343,
        "worksite_location-Longitude": 90.400492,
        "worksite_location-Altitude": 0.0,
        "worksite_location-Accuracy": 35.0,
        "meta-instanceID": "uuid:58f4715f-b5b3-4cee-857d-909d1da12152",
        "KEY": "uuid:58f4715f-b5b3-4cee-857d-909d1da12152",
        "SubmitterID": 311,
        "SubmitterName": "Brick Kiln Survey",
        "ReviewState": "approved",
        "Status": "active"
    },
    {
        "SubmissionDate": "2026-09-20T07:15:20.100Z",
        "survey_date": "2026-09-20",
        "interviewer": "Rahim",
        "division": "2",
        "district": "04",
        "kiln_number": "015",
        "kiln_unique_id": "20405",  # Mismatch: should be 204015
        "kiln_id_display": "20405",
        "worker_name": "Rahim Ali",
        "worker_age": 34,
        "job_zone": "moulding",
        "worker_unique_id": "20405_M",
        "worker_id_display": "20405_M",
        "experience_years": 4.0,
        "daily_hours": 9,
        "ppe_used": "none",
        "resp_symptoms": "frequent",
        "heat_exhaustion": "no",
        "drinking_water_access": "yes",
        "medical_aid_access": "yes",
        "monthly_income": 4800,
        "worksite_location-Latitude": 24.120400,
        "worksite_location-Longitude": 90.310200,
        "meta-instanceID": "uuid:71a2bc44-8831-482f-889a-0012984bb101",
        "KEY": "uuid:71a2bc44-8831-482f-889a-0012984bb101",
        "SubmitterID": 312,
        "SubmitterName": "Brick Kiln Survey"
    },
    {
        "SubmissionDate": "2026-09-20T08:00:10.500Z",
        "survey_date": "2026-09-20",
        "interviewer": "Yasin",
        "division": "1",
        "district": "06",
        "kiln_number": "002",
        "kiln_unique_id": "106002",  # Duplicate Kiln ID with Imran's record
        "kiln_id_display": "106002",
        "worker_name": "Kabir Hossain",
        "worker_age": 38,
        "job_zone": "transport",
        "worker_unique_id": "106002_T",
        "worker_id_display": "106002_T",
        "experience_years": 8.0,
        "daily_hours": 10,
        "ppe_used": "boots",
        "resp_symptoms": "no",
        "heat_exhaustion": "no",
        "drinking_water_access": "yes",
        "medical_aid_access": "yes",
        "monthly_income": 6200,
        "worksite_location-Latitude": 23.777400,
        "worksite_location-Longitude": 90.400500,
        "meta-instanceID": "uuid:cc943211-4d33-4ee2-8233-22b4c9011133",
        "KEY": "uuid:cc943211-4d33-4ee2-8233-22b4c9011133",
        "SubmitterID": 311,
        "SubmitterName": "Brick Kiln Survey"
    },
    {
        "SubmissionDate": "2026-09-21T09:20:00.000Z",
        "survey_date": "2026-09-21",
        "interviewer": "Tanvir",
        "division": "3",
        "district": "12",
        "kiln_number": "008",
        "kiln_unique_id": "312008",
        "kiln_id_display": "312008",
        "worker_name": "Salim Mia",
        "worker_age": 55,  # Review Flag: Age > 45
        "job_zone": "loading",
        "worker_unique_id": "312008_L",
        "worker_id_display": "312008_L",
        "experience_years": 15.0,
        "daily_hours": 8,
        "ppe_used": "gloves",
        "resp_symptoms": "sometimes",
        "heat_exhaustion": "no",
        "drinking_water_access": "yes",
        "medical_aid_access": "yes",
        "monthly_income": 5500,
        "worksite_location-Latitude": 22.340000,
        "worksite_location-Longitude": 91.820000,
        "meta-instanceID": "uuid:dd054322-5e44-4dd3-9344-33c5d0122244",
        "KEY": "uuid:dd054322-5e44-4dd3-9344-33c5d0122244",
        "SubmitterID": 314,
        "SubmitterName": "Brick Kiln Survey"
    },
    {
        "SubmissionDate": "2026-09-21T10:10:00.000Z",
        "survey_date": "2026-09-21",
        "interviewer": "Farhana",
        "division": "2",
        "district": "05",
        "kiln_number": "010",
        "kiln_unique_id": "205010",
        "kiln_id_display": "205010",
        "worker_name": "Jasim Uddin",
        "worker_age": 40,
        "job_zone": "firing",
        "worker_unique_id": "205010_F",
        "worker_id_display": "205010_F",
        "experience_years": 7.0,
        "daily_hours": 11,
        "ppe_used": "mask",
        "resp_symptoms": "frequent",
        "heat_exhaustion": "yes",  # Conflict: heat_exhaustion yes + medical_aid no
        "drinking_water_access": "yes",
        "medical_aid_access": "no",
        "monthly_income": 5100,
        "worksite_location-Latitude": 24.360000,
        "worksite_location-Longitude": 88.620000,
        "meta-instanceID": "uuid:ff276544-7a66-4bb5-1566-55e7f2344466",
        "KEY": "uuid:ff276544-7a66-4bb5-1566-55e7f2344466",
        "SubmitterID": 315,
        "SubmitterName": "Brick Kiln Survey"
    },
    {
        "SubmissionDate": "2026-09-21T11:45:00.000Z",
        "survey_date": "2026-09-21",
        "interviewer": "Yasin",
        "division": "1",
        "district": "06",
        "kiln_number": "003",
        "kiln_unique_id": "106003",
        "kiln_id_display": "106003",
        "worker_name": "Abdul Karim",
        "worker_age": 29,
        "job_zone": "stacking",
        "worker_unique_id": "106003_K",  # Error: job_zone is stacking, expected 106003_S
        "worker_id_display": "106003_K",
        "experience_years": 3.0,
        "daily_hours": 8,
        "ppe_used": "gloves",
        "resp_symptoms": "no",
        "heat_exhaustion": "no",
        "drinking_water_access": "yes",
        "medical_aid_access": "yes",
        "monthly_income": 4500,
        "worksite_location-Latitude": 23.780100,
        "worksite_location-Longitude": 90.410200,
        "meta-instanceID": "uuid:bb832100-3c22-4ff1-9122-11a3b8900022",
        "KEY": "uuid:bb832100-3c22-4ff1-9122-11a3b8900022",
        "SubmitterID": 311,
        "SubmitterName": "Brick Kiln Survey"
    },
    {
        "SubmissionDate": "2026-09-21T13:30:00.000Z",
        "survey_date": "2026-09-21",
        "interviewer": "Nayeem",
        "division": "1",
        "district": "08",
        "kiln_number": "004",
        "kiln_unique_id": "108004",
        "kiln_id_display": "108004",
        "worker_name": "Faruk Hossain",
        "worker_age": 31,
        "job_zone": "sorting",
        "worker_unique_id": "108004_S",
        "worker_id_display": "108004_S",
        "experience_years": 6.0,
        "daily_hours": 15,  # Warning: Extreme hours (>12)
        "ppe_used": "gloves boots",
        "resp_symptoms": "no",
        "heat_exhaustion": "no",
        "drinking_water_access": "yes",
        "medical_aid_access": "yes",
        "monthly_income": 5800,
        "worksite_location-Latitude": 23.950000,
        "worksite_location-Longitude": 90.150000,
        "meta-instanceID": "uuid:aa387655-8b77-4aa6-2677-66f8a3455577",
        "KEY": "uuid:aa387655-8b77-4aa6-2677-66f8a3455577",
        "SubmitterID": 316,
        "SubmitterName": "Brick Kiln Survey"
    },
    {
        "SubmissionDate": "2026-09-22T08:10:00.000Z",
        "survey_date": "2026-09-22",
        "interviewer": "Sultana",
        "division": "1",
        "district": "06",
        "kiln_number": "007",
        "kiln_unique_id": "106007",
        "kiln_id_display": "106007",
        "worker_name": "Shakil Ahmed",
        "worker_age": 16,  # Review Flag: Age < 18
        "job_zone": "carrying",
        "worker_unique_id": "106007_C",
        "worker_id_display": "106007_C",
        "experience_years": 1.0,
        "daily_hours": 8,
        "ppe_used": "none",
        "resp_symptoms": "no",
        "heat_exhaustion": "no",
        "drinking_water_access": "yes",
        "medical_aid_access": "yes",
        "monthly_income": 3800,
        "worksite_location-Latitude": 23.785000,
        "worksite_location-Longitude": 90.412000,
        "meta-instanceID": "uuid:ee165433-6f55-4cc4-0455-44d6e1233355",
        "KEY": "uuid:ee165433-6f55-4cc4-0455-44d6e1233355",
        "SubmitterID": 317,
        "SubmitterName": "Brick Kiln Survey"
    },
    {
        "SubmissionDate": "2026-09-22T09:40:00.000Z",
        "survey_date": "2026-09-22",
        "interviewer": "Yasin",
        "division": "1",
        "district": "06",
        "kiln_number": "009",
        "kiln_unique_id": "106009",
        "kiln_id_display": "106009",
        "worker_name": "Mizanur Rahman",
        "worker_age": 30,
        "job_zone": "firing",
        "worker_unique_id": "106009_F",
        "worker_id_display": "106009_F",
        "experience_years": 7.0,
        "daily_hours": 8,
        "ppe_used": "gloves mask glasses",
        "resp_symptoms": "no",
        "heat_exhaustion": "no",
        "drinking_water_access": "yes",
        "medical_aid_access": "yes",
        "monthly_income": 5400,
        "worksite_location-Latitude": 23.791000,
        "worksite_location-Longitude": 90.419000,
        "meta-instanceID": "uuid:09a12389-11ba-44bb-8877-991122334455",
        "KEY": "uuid:09a12389-11ba-44bb-8877-991122334455",
        "SubmitterID": 311,
        "SubmitterName": "Brick Kiln Survey"
    },
    {
        "SubmissionDate": "2026-09-22T11:00:00.000Z",
        "survey_date": "2026-09-22",
        "interviewer": "Tanvir",
        "division": "3",
        "district": "12",
        "kiln_number": "011",
        "kiln_unique_id": "312011",
        "kiln_id_display": "312011",
        "worker_name": "Anwar Hossain",
        "worker_age": 28,
        "job_zone": "moulding",
        "worker_unique_id": "312011_M",
        "worker_id_display": "312011_M",
        "experience_years": 5.0,
        "daily_hours": 8,
        "ppe_used": "gloves",
        "resp_symptoms": "no",
        "heat_exhaustion": "no",
        "drinking_water_access": "yes",
        "medical_aid_access": "yes",
        "monthly_income": 4900,
        "worksite_location-Latitude": 22.355000,
        "worksite_location-Longitude": 91.834000,
        "meta-instanceID": "uuid:18b23490-22cb-55cc-9988-002233445566",
        "KEY": "uuid:18b23490-22cb-55cc-9988-002233445566",
        "SubmitterID": 314,
        "SubmitterName": "Brick Kiln Survey"
    }
]

db_store.seed_initial_records(INITIAL_SAMPLE_DATA)

# ------------------------------------------------------------------------------
# FastAPI Application & Security
# ------------------------------------------------------------------------------
app = FastAPI(
    title="Brick Kiln Data Quality Monitor",
    description="Enterprise Data Quality, Validation & Review Engine for ODK Central submissions",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

security = HTTPBearer(auto_error=False)

def create_jwt_token(username: str, display_name: str, role: str) -> str:
    payload = {
        "sub": username,
        "name": display_name,
        "role": role,
        "exp": datetime.now(timezone.utc) + timedelta(days=7),
        "iat": datetime.now(timezone.utc)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

async def get_current_user(request: Request, auth: Optional[HTTPAuthorizationCredentials] = Depends(security)) -> Dict[str, Any]:
    token = None
    if auth and auth.credentials:
        token = auth.credentials
    elif "token" in request.cookies:
        token = request.cookies.get("token")
    
    if not token:
        # Check query param for download links
        token = request.query_params.get("token")

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication token required. Please log in."
        )

    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        username = payload.get("sub")
        if username not in USERS_DB:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid user session")
        return {
            "username": username,
            "display_name": payload.get("name", username),
            "role": payload.get("role", "Reviewer")
        }
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Session expired. Please log in again.")
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token signature")

# ------------------------------------------------------------------------------
# Validation Engine
# ------------------------------------------------------------------------------
def evaluate_dataset(submissions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Evaluates submissions against business rules and filters out resolved findings.
    Returns calculated KPIs and active findings table.
    """
    findings = []
    
    # Pre-calculate counts of kiln_unique_id for duplicate detection
    kiln_id_counts = {}
    for doc in submissions:
        k_id = str(doc.get("kiln_unique_id", "")).strip()
        if k_id:
            kiln_id_counts[k_id] = kiln_id_counts.get(k_id, 0) + 1

    corrected_records_count = 0
    records_with_unresolved_issues = set()

    for doc in submissions:
        inst_id = doc.get("meta-instanceID") or doc.get("instance_id") or doc.get("instanceId") or doc.get("KEY", "unknown")
        resolved_flags = doc.get("_resolved_flags", [])
        modified_fields = doc.get("_modified_fields", {})

        raw_worker = doc.get("worker_name") or (doc.get("data", {}).get("worker_name") if isinstance(doc.get("data"), dict) else None)
        raw_interviewer = doc.get("interviewer") or (doc.get("data", {}).get("interviewer") if isinstance(doc.get("data"), dict) else None)
        w_name = str(raw_worker).strip() if raw_worker is not None and str(raw_worker).strip() not in ["", "None"] else "-"
        inv_name = str(raw_interviewer).strip() if raw_interviewer is not None and str(raw_interviewer).strip() not in ["", "None"] else "-"

        if resolved_flags or modified_fields:
            corrected_records_count += 1

        def append_finding(base_dict: Dict[str, Any]):
            base_dict.setdefault("division", doc.get("division"))
            base_dict.setdefault("district", doc.get("district"))
            base_dict.setdefault("kiln_number", doc.get("kiln_number"))
            base_dict.setdefault("kiln_unique_id", doc.get("kiln_unique_id"))
            base_dict.setdefault("job_zone", doc.get("job_zone"))
            base_dict.setdefault("worker_unique_id", doc.get("worker_unique_id"))
            findings.append(base_dict)

        # 1. Kiln ID Formula Verification:
        # Expected kiln_unique_id = Division (1 digit) + District (2 digits) + Kiln Number (3 digits)
        raw_div = doc.get("division")
        raw_dist = doc.get("district")
        raw_kiln = doc.get("kiln_number")
        actual_kiln_id = str(doc.get("kiln_unique_id", "")).strip()

        expected_kiln_id = None
        try:
            if raw_div is not None and raw_dist is not None and raw_kiln is not None:
                d_num = int(str(raw_div).strip())
                dt_num = int(str(raw_dist).strip())
                k_num = int(str(raw_kiln).strip())
                expected_kiln_id = f"{d_num}{dt_num:02d}{k_num:03d}"
        except Exception:
            expected_kiln_id = None

        rule_kiln_mismatch = "rule_kiln_id_mismatch"
        if expected_kiln_id and actual_kiln_id != expected_kiln_id:
            if rule_kiln_mismatch not in resolved_flags:
                records_with_unresolved_issues.add(inst_id)
                append_finding({
                    "finding_id": f"{inst_id}_{rule_kiln_mismatch}",
                    "rule_key": rule_kiln_mismatch,
                    "severity": "ERROR",
                    "category": "errors",
                    "instanceID": inst_id,
                    "field": "kiln_unique_id",
                    "original_value": actual_kiln_id,
                    "detected_issue": f"Kiln ID Structure Mismatch (Reconstructed: {expected_kiln_id}, Found: {actual_kiln_id})",
                    "worker_name": w_name,
                    "survey_date": doc.get("survey_date", "-"),
                    "interviewer": inv_name,
                    "status": "Active"
                })

        # 2. Duplicate Kiln ID Check:
        rule_dup_kiln = "rule_duplicate_kiln_id"
        if actual_kiln_id and kiln_id_counts.get(actual_kiln_id, 0) > 1:
            if rule_dup_kiln not in resolved_flags:
                records_with_unresolved_issues.add(inst_id)
                count_dups = kiln_id_counts[actual_kiln_id]
                append_finding({
                    "finding_id": f"{inst_id}_{rule_dup_kiln}",
                    "rule_key": rule_dup_kiln,
                    "severity": "DUPLICATE_KILN_ID",
                    "category": "duplicates",
                    "instanceID": inst_id,
                    "field": "kiln_unique_id",
                    "original_value": actual_kiln_id,
                    "detected_issue": f"Duplicate Kiln ID detected ({count_dups} submissions share ID {actual_kiln_id})",
                    "worker_name": w_name,
                    "survey_date": doc.get("survey_date", "-"),
                    "interviewer": inv_name,
                    "status": "Active"
                })

        # 3. Worker Unique ID check: {kiln_unique_id}_{JobZoneInitial}
        rule_worker_id = "rule_worker_id_mismatch"
        job_zone = str(doc.get("job_zone", "")).strip()
        worker_uid = str(doc.get("worker_unique_id", "")).strip()
        if job_zone and actual_kiln_id:
            expected_worker_id = f"{actual_kiln_id}_{job_zone[0].upper()}"
            if worker_uid != expected_worker_id:
                if rule_worker_id not in resolved_flags:
                    records_with_unresolved_issues.add(inst_id)
                    append_finding({
                        "finding_id": f"{inst_id}_{rule_worker_id}",
                        "rule_key": rule_worker_id,
                        "severity": "ERROR",
                        "category": "errors",
                        "instanceID": inst_id,
                        "field": "worker_unique_id",
                        "original_value": worker_uid,
                        "detected_issue": f"Worker ID Structure Mismatch (Expected: {expected_worker_id}, Found: {worker_uid})",
                        "worker_name": w_name,
                        "survey_date": doc.get("survey_date", "-"),
                        "interviewer": inv_name,
                        "status": "Active"
                    })

        # 4. Age Boundary Check: worker_age < 18 or > 45
        rule_age = "rule_age_boundary"
        raw_age = doc.get("worker_age")
        if raw_age is not None:
            try:
                age_val = float(raw_age)
                if age_val < 18 or age_val > 45:
                    if rule_age not in resolved_flags:
                        records_with_unresolved_issues.add(inst_id)
                        append_finding({
                            "finding_id": f"{inst_id}_{rule_age}",
                            "rule_key": rule_age,
                            "severity": "REVIEW_FLAG",
                            "category": "reviews",
                            "instanceID": inst_id,
                            "field": "worker_age",
                            "original_value": str(int(age_val) if age_val.is_integer() else age_val),
                            "detected_issue": f"Worker Age Out of Standard Range (<18 or >45: reported {int(age_val)} yrs)",
                            "worker_name": w_name,
                            "survey_date": doc.get("survey_date", "-"),
                            "interviewer": inv_name,
                            "status": "Active"
                        })
            except (ValueError, TypeError):
                pass

        # 5. Health & Medical Aid Conflict: heat_exhaustion == 'yes' AND medical_aid_access == 'no'
        rule_health = "rule_health_aid_conflict"
        heat = str(doc.get("heat_exhaustion", "")).strip().lower()
        med = str(doc.get("medical_aid_access", "")).strip().lower()
        if heat == "yes" and med == "no":
            if rule_health not in resolved_flags:
                records_with_unresolved_issues.add(inst_id)
                append_finding({
                    "finding_id": f"{inst_id}_{rule_health}",
                    "rule_key": rule_health,
                    "severity": "REVIEW_FLAG",
                    "category": "reviews",
                    "instanceID": inst_id,
                    "field": "medical_aid_access",
                    "original_value": f"heat_exhaustion={heat}, medical_aid={med}",
                    "detected_issue": "Health Conflict: Heat exhaustion experienced but no medical aid access reported",
                    "worker_name": w_name,
                    "survey_date": doc.get("survey_date", "-"),
                    "interviewer": inv_name,
                    "status": "Active"
                })

        # 6. Warnings: daily_hours > 12
        rule_hours = "rule_extreme_hours"
        raw_hours = doc.get("daily_hours")
        if raw_hours is not None:
            try:
                hrs = float(raw_hours)
                if hrs > 12:
                    if rule_hours not in resolved_flags:
                        append_finding({
                            "finding_id": f"{inst_id}_{rule_hours}",
                            "rule_key": rule_hours,
                            "severity": "WARNING",
                            "category": "warnings",
                            "instanceID": inst_id,
                            "field": "daily_hours",
                            "original_value": str(hrs),
                            "detected_issue": f"Extreme Daily Work Hours ({hrs} hrs/day exceeds 12-hour threshold)",
                            "worker_name": w_name,
                            "survey_date": doc.get("survey_date", "-"),
                            "interviewer": inv_name,
                            "status": "Active"
                        })
            except (ValueError, TypeError):
                pass


    total_submissions = len(submissions)
    
    # Valid Records: Submissions with zero open errors and zero open review flags
    valid_records_count = max(0, total_submissions - len(records_with_unresolved_issues))
    
    open_errors_count = sum(1 for f in findings if f["severity"] == "ERROR")
    dup_kiln_count = sum(1 for f in findings if f["severity"] == "DUPLICATE_KILN_ID")
    review_flags_count = sum(1 for f in findings if f["severity"] == "REVIEW_FLAG")
    warnings_count = sum(1 for f in findings if f["severity"] == "WARNING")

    return {
        "kpi": {
            "total_submissions": total_submissions,
            "valid_records": valid_records_count,
            "open_errors": open_errors_count,
            "duplicate_kiln_ids": dup_kiln_count,
            "review_flags": review_flags_count,
            "corrected_records": corrected_records_count,
            "warnings": warnings_count
        },
        "findings": findings
    }

# ------------------------------------------------------------------------------
# Pydantic Request Models
# ------------------------------------------------------------------------------
class LoginRequest(BaseModel):
    username: str
    password: str

class ResolveFindingRequest(BaseModel):
    instanceID: str
    finding_id: str
    rule_key: Optional[str] = None
    action: str  # 'approve' | 'edit'
    field: Optional[str] = None
    new_value: Optional[Any] = None
    reason: Optional[str] = "Approved via Quality Monitor"
    cascading_updates: Optional[Dict[str, Any]] = None

# ------------------------------------------------------------------------------
# API Endpoints
# ------------------------------------------------------------------------------

@app.post("/api/login")
async def api_login(req: LoginRequest):
    username = req.username.strip()
    user_record = USERS_DB.get(username)
    if not user_record:
        raise HTTPException(status_code=401, detail="Invalid username or password")
    
    # Verify password with bcrypt
    if not bcrypt.checkpw(req.password.encode("utf-8"), user_record["password_hash"].encode("utf-8")):
        raise HTTPException(status_code=401, detail="Invalid username or password")

    token = create_jwt_token(username, user_record["display_name"], user_record["role"])
    
    response = JSONResponse(content={
        "token": token,
        "username": username,
        "display_name": user_record["display_name"],
        "role": user_record["role"]
    })
    # Set HTTP-only cookie as well for browser persistence
    response.set_cookie(key="token", value=token, httponly=False, max_age=86400 * 7, samesite="lax")
    return response


@app.get("/api/me")
async def api_me(user: Dict[str, Any] = Depends(get_current_user)):
    return user


@app.get("/api/run-validation")
async def api_run_validation(user: Dict[str, Any] = Depends(get_current_user)):
    """
    Evaluates all records in cleaned_submissions, calculates KPI metrics,
    identifies unresolved flags, and returns summary + findings array.
    """
    submissions = db_store.get_clean_all()
    results = evaluate_dataset(submissions)
    results["db_mode"] = "MongoDB Live" if db_store.is_connected_to_mongo else "Local Resilient Store"
    results["evaluated_at"] = datetime.now(timezone.utc).isoformat()
    return results


@app.post("/api/resolve-finding")
async def api_resolve_finding(payload: ResolveFindingRequest, user: Dict[str, Any] = Depends(get_current_user)):
    """
    Accepts payload { instanceID, finding_id, action: 'approve' | 'edit', field, new_value, user, cascading_updates }.
    Logs to audit_logs, updates cleaned_submissions, marks finding resolved.
    Enforces Cascading ID Validation & Interrelated Field Synchronization for Kiln and Worker IDs.
    """
    current_rec = db_store.get_clean_by_instance(payload.instanceID)
    if not current_rec:
        raise HTTPException(status_code=404, detail=f"Record {payload.instanceID} not found.")

    finding_key = payload.rule_key or payload.finding_id
    field_name = payload.field or "general"
    old_value = current_rec.get(field_name, "")
    actor = user.get("username", "reviewer")

    if payload.action == "approve":
        # Approve as-is: keep existing value, add rule_key to _resolved_flags
        db_store.update_clean_record(
            instance_id=payload.instanceID,
            updates={},
            resolved_flag=finding_key
        )
        # Log to audit trail
        db_store.add_audit_log({
            "instanceID": payload.instanceID,
            "action": "APPROVED_AS_IS",
            "field": field_name,
            "old_value": str(old_value),
            "new_value": str(old_value),
            "changed_by": actor,
            "notes": payload.reason or "Record approved as-is after verification.",
            "rule_resolved": finding_key
        })
        logger.info("Record %s approved as-is by %s", payload.instanceID, actor)
        return {"status": "success", "message": "Finding marked as approved as-is.", "instanceID": payload.instanceID}

    elif payload.action == "edit":
        # Check if Kiln ID related or cascading updates passed
        is_kiln_id_edit = (
            (payload.field in ["kiln_unique_id", "division", "district", "kiln_number"]) or
            (payload.rule_key in ["rule_duplicate_kiln_id", "rule_kiln_id_mismatch", "rule_worker_id_mismatch"]) or
            bool(payload.cascading_updates)
        )

        updates = {}
        affected_fields = []
        resolved_rules = [finding_key]

        if is_kiln_id_edit:
            casc = payload.cascading_updates or {}
            
            # Baseline values from current record
            curr_div = current_rec.get("division")
            curr_dist = current_rec.get("district")
            curr_kiln = current_rec.get("kiln_number")
            curr_kiln_id = str(current_rec.get("kiln_unique_id", "")).strip()
            curr_worker_id = str(current_rec.get("worker_unique_id", "")).strip()

            target_kiln_id = None
            if "kiln_unique_id" in casc and str(casc["kiln_unique_id"]).strip():
                target_kiln_id = str(casc["kiln_unique_id"]).strip()
            elif payload.field == "kiln_unique_id" and payload.new_value is not None:
                target_kiln_id = str(payload.new_value).strip()

            if target_kiln_id and len(target_kiln_id) == 6 and target_kiln_id.isdigit():
                # Option B: Direct full Kiln ID breakdown
                new_div = int(target_kiln_id[0])
                new_dist = target_kiln_id[1:3]
                new_kiln = target_kiln_id[3:6]
                final_kiln_id = target_kiln_id
            else:
                # Option A: Component-level inputs
                raw_d = casc.get("division", payload.new_value if payload.field == "division" else curr_div)
                raw_dt = casc.get("district", payload.new_value if payload.field == "district" else curr_dist)
                raw_k = casc.get("kiln_number", payload.new_value if payload.field == "kiln_number" else curr_kiln)

                try:
                    d_int = int(str(raw_d).strip())
                    dt_int = int(str(raw_dt).strip())
                    k_int = int(str(raw_k).strip())
                    new_div = d_int
                    new_dist = f"{dt_int:02d}"
                    new_kiln = f"{k_int:03d}"
                    final_kiln_id = f"{new_div}{new_dist}{new_kiln}"
                except Exception:
                    final_kiln_id = str(target_kiln_id or curr_kiln_id)
                    new_div = raw_d
                    new_dist = raw_dt
                    new_kiln = raw_k

            # Job zone suffix derivation
            suffix = None
            if curr_worker_id and "_" in curr_worker_id:
                parts = curr_worker_id.rsplit("_", 1)
                if len(parts) == 2 and parts[1]:
                    suffix = parts[1]
            if not suffix:
                jz = str(current_rec.get("job_zone", "")).strip()
                suffix = jz[0].upper() if jz else "F"

            final_worker_id = f"{final_kiln_id}_{suffix}"

            updates = {
                "division": new_div,
                "district": new_dist,
                "kiln_number": new_kiln,
                "kiln_unique_id": final_kiln_id,
                "worker_unique_id": final_worker_id
            }
            affected_fields = ["division", "district", "kiln_number", "kiln_unique_id", "worker_unique_id"]

            resolved_rules = list(set([
                finding_key,
                "rule_duplicate_kiln_id",
                "rule_kiln_id_mismatch",
                "rule_worker_id_mismatch"
            ]))
        else:
            if payload.field is None or payload.new_value is None:
                raise HTTPException(status_code=400, detail="Field name and new_value required for edit action.")
            
            new_val = payload.new_value
            if payload.field in ["worker_age", "daily_hours", "monthly_income"]:
                try:
                    new_val = float(new_val) if "." in str(new_val) else int(new_val)
                except Exception:
                    pass
            updates = {payload.field: new_val}
            affected_fields = [payload.field]

        # Atomically update cleaned_submissions
        db_store.update_clean_record(
            instance_id=payload.instanceID,
            updates=updates,
            resolved_flag=resolved_rules
        )

        # Audit logs for all affected fields
        for f in affected_fields:
            old_f_val = current_rec.get(f, "")
            new_f_val = updates.get(f, "")
            # Log if value changed or if it was the explicitly targeted field
            if str(old_f_val) != str(new_f_val) or f == (payload.field or "kiln_unique_id"):
                action_name = "EDITED_AND_CORRECTED" if f == (payload.field or "kiln_unique_id") else "CASCADING_FIELD_UPDATE"
                note_text = payload.reason or "Value corrected by reviewer."
                if f != (payload.field or "kiln_unique_id"):
                    note_text = f"Cascading synchronization from Kiln ID ({updates.get('kiln_unique_id')}). {note_text}"

                db_store.add_audit_log({
                    "instanceID": payload.instanceID,
                    "action": action_name,
                    "field": f,
                    "old_value": str(old_f_val) if old_f_val is not None else "",
                    "new_value": str(new_f_val),
                    "changed_by": actor,
                    "notes": note_text,
                    "rule_resolved": finding_key
                })

        logger.info("Record %s updated with fields %s by %s", payload.instanceID, updates, actor)
        return {
            "status": "success",
            "message": "Kiln ID and interrelated fields synchronized successfully.",
            "updated_fields": updates,
            "instanceID": payload.instanceID
        }


    else:
        raise HTTPException(status_code=400, detail="Invalid action. Must be 'approve' or 'edit'.")


@app.post("/api/sync-odk")
async def api_sync_odk(user: Dict[str, Any] = Depends(get_current_user)):
    """
    1. Authenticate with ODK Central API (POST /v1/sessions).
    2. Download submissions from /v1/projects/{ODK_PROJECT_ID}/forms/{ODK_FORM_ID}.svc/Submissions?$format=json.
    3. Upsert original data into raw_submissions.
    4. Upsert into cleaned_submissions without overwriting fields previously modified by users.
    """
    actor = user.get("username", "system")
    auth_url = f"{ODK_BASE_URL}/v1/sessions"
    submissions_url = f"{ODK_BASE_URL}/v1/projects/{ODK_PROJECT_ID}/forms/{ODK_FORM_ID}.svc/Submissions?$format=json"

    # Check for placeholder configuration
    is_placeholder_url = "odk.yourdomain.com" in ODK_BASE_URL or "example.com" in ODK_EMAIL

    if is_placeholder_url:
        # Provide simulated sync with new or refreshed sample records
        sample_count = len(INITIAL_SAMPLE_DATA)
        msg = f"Demonstration Sync: Live ODK endpoint is placeholder ({ODK_BASE_URL}). Verified {sample_count} submissions against pipeline."
        db_store.add_sync_log({
            "status": "SIMULATED_SUCCESS",
            "records_synced": sample_count,
            "message": msg,
            "user": actor,
            "odk_endpoint": ODK_BASE_URL
        })
        return {
            "status": "success",
            "mode": "simulated",
            "records_synced": sample_count,
            "message": msg
        }

    ODK_DEFAULT_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*"
    }

    try:
        # Step 1: Authenticate with ODK Central
        auth_resp = requests.post(
            auth_url,
            json={"email": ODK_EMAIL, "password": ODK_PASSWORD},
            headers=ODK_DEFAULT_HEADERS,
            timeout=15
        )

        if auth_resp.status_code != 200:
            error_msg = f"ODK Authentication failed (HTTP {auth_resp.status_code}): {auth_resp.text[:120]}"
            db_store.add_sync_log({
                "status": "FAILED",
                "records_synced": 0,
                "message": error_msg,
                "user": actor,
                "odk_endpoint": ODK_BASE_URL
            })
            raise HTTPException(status_code=502, detail=error_msg)

        auth_data = auth_resp.json()
        token = auth_data.get("token")

        # Step 2: Fetch Submissions via OData JSON endpoint
        headers = {
            **ODK_DEFAULT_HEADERS,
            "Authorization": f"Bearer {token}"
        }
        sub_resp = requests.get(submissions_url, headers=headers, timeout=30)

        if sub_resp.status_code != 200:
            error_msg = f"Failed to retrieve submissions from ODK (HTTP {sub_resp.status_code}): {sub_resp.text[:120]}"
            db_store.add_sync_log({
                "status": "FAILED",
                "records_synced": 0,
                "message": error_msg,
                "user": actor,
                "odk_endpoint": ODK_BASE_URL
            })
            raise HTTPException(status_code=502, detail=error_msg)

        res_json = sub_resp.json()
        items = res_json.get("value", []) if isinstance(res_json, dict) else res_json
        if not isinstance(items, list):
            items = []

        # Step 3 & 4: Upsert raw & cleaned submissions
        synced_count = 0
        for item in items:
            inst_id = (
                item.get("__id") or 
                item.get("meta-instanceID") or 
                item.get("instance_id") or 
                item.get("KEY") or 
                item.get("instanceId")
            )
            if not inst_id:
                continue
            item["meta-instanceID"] = str(inst_id)
            db_store.upsert_raw(dict(item))
            db_store.upsert_clean(dict(item))
            synced_count += 1

        db_store.add_sync_log({
            "status": "SUCCESS",
            "records_synced": synced_count,
            "message": f"Successfully pulled {synced_count} live submissions from ODK Central ({ODK_FORM_ID}).",
            "user": actor,
            "odk_endpoint": ODK_BASE_URL
        })

        return {
            "status": "success",
            "records_synced": synced_count,
            "message": f"Successfully synced {synced_count} live submissions from ODK Central."
        }

    except requests.exceptions.RequestException as e:
        error_msg = f"ODK Connection Error: {str(e)}"
        db_store.add_sync_log({
            "status": "ERROR",
            "records_synced": 0,
            "message": error_msg,
            "user": actor,
            "odk_endpoint": ODK_BASE_URL
        })
        raise HTTPException(status_code=503, detail=error_msg)


@app.get("/api/submissions")
async def api_get_submissions(
    search: Optional[str] = None,
    job_zone: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    user: Dict[str, Any] = Depends(get_current_user)
):
    """
    Submissions Registry: List records with search & filtering
    """
    records = db_store.get_clean_all()

    # Filtering
    if search:
        s = search.lower().strip()
        records = [
            r for r in records if (
                s in str(r.get("worker_name", "")).lower() or
                s in str(r.get("kiln_unique_id", "")).lower() or
                s in str(r.get("interviewer", "")).lower() or
                s in str(r.get("meta-instanceID", "")).lower() or
                s in str(r.get("worker_unique_id", "")).lower()
            )
        ]

    if job_zone and job_zone != "all":
        records = [r for r in records if str(r.get("job_zone", "")).lower() == job_zone.lower()]

    total = len(records)
    paginated = records[offset : offset + limit]

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "records": paginated
    }


@app.get("/api/submissions/{instance_id:path}")
async def api_get_submission_by_id(instance_id: str, user: Dict[str, Any] = Depends(get_current_user)):
    rec = db_store.get_clean_by_instance(instance_id)
    if not rec:
        raise HTTPException(status_code=404, detail=f"Submission {instance_id} not found.")
    return rec


@app.get("/api/audit-logs")
async def api_get_audit_logs(user: Dict[str, Any] = Depends(get_current_user)):
    return db_store.get_audit_logs(limit=100)


@app.get("/api/sync-logs")
async def api_get_sync_logs(user: Dict[str, Any] = Depends(get_current_user)):
    return {
        "logs": db_store.get_sync_logs(limit=50),
        "config": {
            "odk_base_url": ODK_BASE_URL,
            "odk_project_id": ODK_PROJECT_ID,
            "odk_form_id": ODK_FORM_ID,
            "odk_email": ODK_EMAIL
        }
    }


# ------------------------------------------------------------------------------
# Export Endpoints
# ------------------------------------------------------------------------------

@app.get("/api/export/clean-csv")
async def export_clean_csv(user: Dict[str, Any] = Depends(get_current_user)):
    """
    Download the clean dataset as CSV.
    """
    records = db_store.get_clean_all()
    if not records:
        df = pd.DataFrame()
    else:
        # Strip internal tracking fields
        cleaned_list = []
        for r in records:
            item = dict(r)
            item.pop("_id", None)
            item.pop("_resolved_flags", None)
            item.pop("_modified_fields", None)
            cleaned_list.append(item)
        df = pd.DataFrame(cleaned_list)

    csv_data = df.to_csv(index=False)
    filename = f"brick_kiln_clean_submissions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    return StreamingResponse(
        iter([csv_data]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@app.get("/api/export/audit-csv")
async def export_audit_csv(user: Dict[str, Any] = Depends(get_current_user)):
    """
    Download CSV containing only modified rows with wide paired columns ({field}_old, {field}_new) and changed_by.
    """
    audit_logs = db_store.get_audit_logs(limit=1000)
    
    # Group changes by instanceID
    grouped: Dict[str, Dict[str, Any]] = {}
    for entry in audit_logs:
        inst_id = entry.get("instanceID")
        if not inst_id:
            continue
        if inst_id not in grouped:
            grouped[inst_id] = {
                "instanceID": inst_id,
                "changed_by": entry.get("changed_by", ""),
                "last_modified": entry.get("created_at", entry.get("timestamp", ""))
            }
        
        field = entry.get("field", "unknown")
        old_val = entry.get("old_value", "")
        new_val = entry.get("new_value", "")
        
        # Paired columns {field}_old, {field}_new
        grouped[inst_id][f"{field}_old"] = old_val
        grouped[inst_id][f"{field}_new"] = new_val

    if not grouped:
        df = pd.DataFrame(columns=["instanceID", "changed_by", "last_modified", "field_old", "field_new"])
    else:
        df = pd.DataFrame(list(grouped.values()))

    csv_data = df.to_csv(index=False)
    filename = f"brick_kiln_audit_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    return StreamingResponse(
        iter([csv_data]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@app.get("/api/export/r-script")
async def export_r_script():
    """
    Download a pre-configured standalone R cleaning script.
    """
    script_path = os.path.join(os.path.dirname(__file__), "clean_script.R")
    if not os.path.exists(script_path):
        raise HTTPException(status_code=404, detail="clean_script.R not found")
    return FileResponse(
        path=script_path,
        media_type="text/plain",
        filename="clean_script.R"
    )


# ------------------------------------------------------------------------------
# Dashboard HTML Route
# ------------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    html_path = os.path.join(os.path.dirname(__file__), "templates", "index.html")
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse("<h1>Brick Kiln Data Quality Monitor</h1><p>Dashboard template loading...</p>")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=PORT, reload=True)
