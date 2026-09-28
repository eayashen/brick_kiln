# Brick Kiln Data Quality Monitor & Review Platform

A full-stack, responsive Data Quality & Review Web Application built for monitoring brick kiln worker field survey data. Features live ODK Central API synchronization, MongoDB Atlas storage, rule-based data validation, audit trail tracking, and CSV/R script export utilities.

---

## 🌟 Key Features

- **Live ODK Central Integration**: Direct authentication and sync with ODK Central REST & OData APIs.
- **Dual-Layer Database Architecture**:
  - `raw_submissions`: Immutable raw survey submissions fetched directly from ODK Central.
  - `cleaned_submissions`: Dynamic dataset reflecting verified, approved, and corrected entries.
  - `audit_logs`: Detailed audit trail tracking every modification with timestamp, user identity, old/new values, and reason.
- **Rule-Based Validation Engine**:
  - Duplicate submission checks.
  - Worker ID syntax & structure verification.
  - Plausibility boundaries (age, extreme work hours, health aid logic).
- **Interactive Review & Resolution Workflow**:
  - Review flagged records with full survey metadata.
  - One-click approval or inline correction with mandatory justification logging.
- **Role-Based Access Control**:
  - Secure JWT authentication with predefined roles (`admin`, `reviewer`).
- **Comprehensive Exports**:
  - Export cleaned data and audit trails to CSV.
  - Downloadable automated R cleaning script (`clean_script.R`).

---

## 🚀 Quick Start

### 1. Prerequisites
- Python 3.9+
- MongoDB Atlas cluster or local MongoDB instance
- Access credentials to ODK Central

### 2. Installation
Clone the repository and install the dependencies:
```bash
git clone <repository_url>
cd Dashboard
pip install -r requirements.txt
```

### 3. Environment Configuration
Copy `.env.example` to `.env` and fill in your credentials:
```bash
cp .env.example .env
```
Edit `.env`:
```env
MONGODB_URI=mongodb+srv://<user>:<password>@cluster0.mongodb.net/?retryWrites=true&w=majority
DB_NAME=odk_data_quality
ODK_BASE_URL=https://<your-subdomain>.getodk.cloud
ODK_PROJECT_ID=5
ODK_FORM_ID=brick_kiln_survey
ODK_EMAIL=<your-odk-email>
ODK_PASSWORD=<your-odk-password>
JWT_SECRET_KEY=<your-secret-key>
```

### 4. Running the Application
Start the FastAPI server using Uvicorn:
```bash
python -m uvicorn app:app --host 0.0.0.0 --port 8000 --reload
```
Open your browser and navigate to:
```
http://localhost:8000
```

### Default Login Accounts:
| Username | Password | Role |
| :--- | :--- | :--- |
| `eayashen` | `kiln1234` | Admin |
| `meftah` | `kiln4321` | Reviewer |

---

## 📁 Project Structure

```
├── app.py                 # FastAPI backend server & validation engine
├── clean_script.R         # Standalone R data cleaning pipeline
├── templates/
│   └── index.html         # Responsive SPA dashboard (TailwindCSS, Dark/Light modes)
├── requirements.txt       # Python package dependencies
├── .env.example           # Environment template (safe for version control)
├── .gitignore             # Git ignore rules protecting secrets & temporary files
└── README.md              # Project documentation
```

---

## 🔒 Security Notice
Never commit the `.env` file or local submission dumps (`brick_kiln_local_store.json`) to version control. Ensure `.gitignore` is active before pushing to remote repositories.
