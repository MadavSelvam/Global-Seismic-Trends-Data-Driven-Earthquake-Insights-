# Global Seismic Trends — Streamlit Dashboard

## 1. Place the files in your project
Copy `app.py` and `requirements.txt` into the root of your project:
`D:\global seismic trends`

No `src/` folder is required.

## 2. Activate your virtual environment
In Git Bash:
```bash
cd "/d/global seismic trends"
source .venv/Scripts/activate
```

If activation fails in Git Bash, use the VS Code terminal with your existing `.venv` interpreter, or run:
```bash
.venv\Scripts\activate
```
in Command Prompt.

## 3. Install dependencies
```bash
python -m pip install -r requirements.txt
```

## 4. Start MySQL
Ensure MySQL Server is running and the database `global_seismic_trends` contains the `earthquakes` table.

## 5. Run the dashboard
```bash
python -m streamlit run app.py
```
Streamlit will display a local URL, normally `http://localhost:8501`.

## 6. Connect
In the sidebar, enter:
- Host: `localhost`
- Port: `3306`
- Database: `global_seismic_trends`
- Username: your MySQL username
- Password: your MySQL password

## Notes
- The app reads from MySQL and does not insert, update, or delete database records.
- The map is capped at 5,000 filtered records for performance.
- Credentials are entered in the app UI and are not stored in this source file.
- The app assumes the project table uses the USGS-derived columns `id`, `time`, `latitude`, `longitude`, `mag`, `depth_km`, `place`, `tsunami`, and optionally `net`.
