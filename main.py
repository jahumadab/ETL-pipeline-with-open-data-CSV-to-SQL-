import requests
import pandas as pd

# --------------------------- Config ---------------------------
PEOPLE_BASE = "https://www.swapi.tech/api/people/"
VEHICLE_BASE = "https://www.swapi.tech/api/vehicles/"
STARSHIP_BASE = "https://www.swapi.tech/api/starships/"
TIMEOUT = 10

# Funcion que extrae ultimo segmento ID
def last_id(url: str):
    if not url:
        return None
    last = url.rstrip('/').split('/')[-1]
    return int(last) if last.isdigit() else None  # int o None

# Funcion que consume vehicles/starships
def fetch_catalog(base_url, ids, id_col_name, fields, timeout=10):
    """
    base_url: 'https://www.swapi.tech/api/vehicles/' o '.../starships/'
    ids:      lista de enteros (IDs a consultar)
    id_col_name: 'vehicle_id' o 'starship_id'
    fields:   lista de campos a traer desde 'properties' (p.ej. ['name','model',...])
    """
    rows = []
    for _id in sorted({int(i) for i in ids if pd.notna(i)}):
        resp = requests.get(f"{base_url}{_id}", timeout=timeout)
        if not resp.ok:
            continue
        data = resp.json()
        if data.get("message") != "ok":
            continue

        result = data["result"]
        props = result.get("properties", {})

        # Preferimos el uid del API si es numérico; si no, usamos _id
        try:
            api_uid = int(result.get("uid"))
        except (TypeError, ValueError):
            api_uid = _id

        row = {id_col_name: api_uid, "url": props.get("url")}
        for f in fields:
            row[f] = props.get(f)
        rows.append(row)

    return pd.DataFrame(rows)


# ======================= 1) Personas ==========================
people_rows = []
for i in range(1, 83):  # ids 1..82
    try:
        r = requests.get(f"{PEOPLE_BASE}{i}", timeout=TIMEOUT)
        if not r.ok:
            continue
        data = r.json()
        if data.get("message") != "ok":
            continue
        result = data["result"]
        props = result.get("properties", {})
        people_rows.append({
            "uid": result.get("uid"),
            "name": props.get("name"),
            "gender": props.get("gender"),
            "height": props.get("height"),
            "vehicles": props.get("vehicles") or [],
            "starships": props.get("starships") or [],
        })
    except Exception:
        continue

people_df = pd.DataFrame(people_rows, columns=["uid", "name", "gender", "height", "vehicles", "starships"])

# ================== 2) Tablas normalizadas ====================
# people (sin listas)
people = people_df[["uid", "name", "gender", "height"]].copy()
people.to_csv("people.csv", index=False)

# person_vehicles (N:M)
pv = people_df[["uid", "vehicles"]].explode("vehicles", ignore_index=True)
pv = pv.dropna(subset=["vehicles"])
pv["vehicle_id"] = pv["vehicles"].apply(last_id)
pv = pv.dropna(subset=["vehicle_id"])
person_vehicles = pv[["uid", "vehicle_id"]].drop_duplicates()
person_vehicles["vehicle_id"] = person_vehicles["vehicle_id"].astype(int)
person_vehicles.to_csv("person_vehicles.csv", index=False)

# person_starships (N:M)
ps = people_df[["uid", "starships"]].explode("starships", ignore_index=True)
ps = ps.dropna(subset=["starships"])
ps["starship_id"] = ps["starships"].apply(last_id)
ps = ps.dropna(subset=["starship_id"])
person_starships = ps[["uid", "starship_id"]].drop_duplicates()
person_starships["starship_id"] = person_starships["starship_id"].astype(int)
person_starships.to_csv("person_starships.csv", index=False)

# ================== 3) Dimensiones (catálogos) =================
vehicle_fields = [
    "name", "model", "manufacturer", "cost_in_credits",
    "cargo_capacity", "passengers", "max_atmosphering_speed",
    "crew", "length", "consumables", "vehicle_class",
]
starship_fields = [
    "name", "model", "manufacturer", "cost_in_credits",
    "cargo_capacity", "passengers", "max_atmosphering_speed",
    "crew", "length", "consumables", "starship_class",
    "hyperdrive_rating", "MGLT",
]

vehicle_ids = person_vehicles["vehicle_id"].unique().tolist()
starship_ids = person_starships["starship_id"].unique().tolist()

vehicles = fetch_catalog(VEHICLE_BASE, vehicle_ids, "vehicle_id", vehicle_fields)
starships = fetch_catalog(STARSHIP_BASE, starship_ids, "starship_id", starship_fields)

vehicles.to_csv("vehicles.csv", index=False)
starships.to_csv("starships.csv", index=False)

print("Listo: people.csv, person_vehicles.csv, person_starships.csv, vehicles.csv, starships.csv")
