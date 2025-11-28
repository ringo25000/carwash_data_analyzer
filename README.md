## Setup + Run (per machine)



```bash
# Clone Repo
git clone <repo_link> 
cd carwash_data_analyzer


# MAC
python3 -m venv .venv
source .venv/bin/activate


# WINDOWS(POWERSHELL)
python -m venv .venv
.\.venv\Scripts\Activate.ps1


# INSTALL DEPENDANCIES
pip install -r requirements.txt


# Install Playwright browser (Chromium)
python -m playwright install chromium



### RUNNING PROJECT ###

# Run login Script
python -m scripts.cryptopay_login

# Run the full ETL pipeline (scrape → clean → load DB)
python -m app.pipeline

