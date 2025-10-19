import requests
import pandas as pd 


api_url = "https://www.swapi.tech/api/people/"
rows = []



for i in range(0,82):
    person_url = f"{api_url}{i+1}"
    person_response = requests.get(person_url)
    person_data = person_response.json()

    if(person_data["message"] == "ok"):
               
               result = person_data["result"]
               props = result.get("properties", {})
               rows.append({
                    "uid": result.get("uid"),
                    "name": props.get("name"),
                    "gender": props.get("gender"),
                    "height": props.get("height"),
                    "vehicles": props.get("vehicles"),   # suele ser lista
                    "starships": props.get("starships"), # suele ser lista
               })
    

df = pd.DataFrame(rows, columns=["uid", "name", "gender", "height", "vehicles", "starships"])

df.to_csv('person_star_wars.csv', index=False)